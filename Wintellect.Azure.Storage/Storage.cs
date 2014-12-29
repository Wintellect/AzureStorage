/******************************************************************************
Module:  Storage.cs
Notices: Copyright (c) by Jeffrey Richter & Wintellect
******************************************************************************/

using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Blob.Protocol;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Wintellect.Azure.Storage.Table;

namespace Wintellect.Azure.Storage {
   /// <summary>Defines extensions methods useful with Azure storage.</summary>
   public static class StorageExtensions {
      #region Primitive type extensions
      /// <summary>Improves performance when talking to Azure storage by setting UseNagleAlorithm to false, Expect100Continue to false, and setting the specific connection limit.</summary>
      /// <param name="uri">The specific URI to apply these performance improvements to; if null, these performance improvements are applied to all URIs.</param>
      /// <param name="connectionLimit">The connection limit.</param>
      public static void ImprovePerformance(this Uri uri, Int32 connectionLimit = 100) {
         if (uri != null) {
            // Improve performance for this 1 Uri
            ServicePoint sp = ServicePointManager.FindServicePoint(uri);
            sp.UseNagleAlgorithm = false;
            sp.Expect100Continue = false;
            sp.ConnectionLimit = connectionLimit;
            return;
         } else {
            // Improve performance for all Uri
            ServicePointManager.DefaultConnectionLimit = connectionLimit;

            // Turn off "expect 100 continue" so PUT/POST requests don't send expect
            // header & wait for 100-continue from server before sending payload body
            ServicePointManager.Expect100Continue = false;

            // Turn off the Nagle algorithm to send small packets quickly
            ServicePointManager.UseNagleAlgorithm = false;
         }
      }

#if DeleteMe
      [DebuggerStepThrough]
      public static HttpWebResponse WebResponse(this WebException webException) {
         return (HttpWebResponse)webException.Response;
      }
#endif
      #endregion

      #region Methods that apply to Blobs, Tables, and Queues
      /// <summary>Compares a StorageException against a specific HTTP code and storage error code string.</summary>
      /// <param name="se">The StorageException.</param>
      /// <param name="code">The HTTP code to compare against.</param>
      /// <param name="storageErrorCode">The optional storage error code string.</param>
      /// <returns>True if the StorageException is due to the specific HTTP code and storage error code string.</returns>
      public static Boolean Matches(this StorageException se, HttpStatusCode code, String storageErrorCode = null) {
         if ((HttpStatusCode)se.RequestInformation.HttpStatusCode != code) return false;
         if (storageErrorCode == null) return true;
         return se.RequestInformation.ExtendedErrorInformation.ErrorCode == storageErrorCode;
      }

#if DeleteMe

      public static OperationContext ChangeRequestVersionHeader(this OperationContext context, String versionHeaderValue) {
         context.SendingRequest += (sender, e) => {
            ((HttpWebRequest)e.Request).Headers["x-ms-version"] = versionHeaderValue;
         };
         return context;
      }
#endif

      [DebuggerStepThrough]
      private static async Task<TStorage> RetryUntilCreatedAsync<TStorage>(TStorage storage, Func<TStorage, Task> action, Int32? msBetweenTries = null) {
         while (true) {
            try { await action(storage).ConfigureAwait(false); return storage; }
            catch (StorageException ex) {
               if (ex.RequestInformation.HttpStatusCode != 0/*StorageErrorCode.ResourceAlreadyExists*/) throw;
            }
            await Task.Delay(msBetweenTries ?? 10).ConfigureAwait(false);
         }
      }

      /// <summary>Retries an operation against a CloudBlobContainer until it succeeds.</summary>
      /// <param name="container">The blob container.</param>
      /// <param name="msBetweenTries">The time to wait between retries.</param>
      /// <returns>The passed-in CloudBlobContainer.</returns>
      [DebuggerStepThrough]
      public static Task<CloudBlobContainer> RetryUntilCreatedAsync(this CloudBlobContainer container, Int32? msBetweenTries = null) {
         return RetryUntilCreatedAsync<CloudBlobContainer>(container, c => c.CreateIfNotExistsAsync(), msBetweenTries);
      }

      /// <summary>Retries an operation against an AzureTable until it succeeds.</summary>
      /// <param name="table">The table.</param>
      /// <param name="msBetweenTries">The time to wait between retries.</param>
      /// <returns>The passed-in AzureTable.</returns>
      [DebuggerStepThrough]
      public static Task<AzureTable> RetryUntilCreatedAsync(this AzureTable table, Int32? msBetweenTries = null) {
         return RetryUntilCreatedAsync<AzureTable>(table, tc => tc.CreateIfNotExistsAsync(), msBetweenTries);
      }
      #endregion
   }

   /// <summary>Elects 1 machine to perform some operation periodically.</summary>

   public static class PeriodicElector {      
      private enum ElectionError {
         Unknown,
         BlobNotFound,
         LeaseAlreadyPresent,
         ElectionOver
      }
      /// <summary>Ensures that 1 machine will execute an operation periodically.</summary>
      /// <param name="startTime">The base time; all machines should use the same exact base time.</param>
      /// <param name="period">Indicates how frequently you want the operation performed.</param>
      /// <param name="timeBetweenLeaseRetries">Indicates how frequently a machine that is not elected to perform the work should retry the work in case the elected machine crashes while performing the work.</param>
      /// <param name="blob">The blob that all the machines should be using to acquire a lease.</param>
      /// <param name="cancellationToken">Indicates when the operation should no longer be performed in the future.</param>
      /// <param name="winnerWorker">A method that is called periodically by whatever machine wins the election.</param>
      /// <returns>A Task which you can  use to catch an exception or known then this method has been canceled.</returns>
      public static async Task RunAsync(DateTimeOffset startTime, TimeSpan period, 
         TimeSpan timeBetweenLeaseRetries, ICloudBlob blob, 
         CancellationToken cancellationToken, Func<Task> winnerWorker) {
         var bro = new BlobRequestOptions { RetryPolicy = new ExponentialRetry() };
         const Int32 leaseDurationSeconds = 60; // 15-60 seconds
         DateTimeOffset nextElectionTime = NextElectionTime(startTime, period);
         while (true) {
            TimeSpan timeToWait = nextElectionTime - DateTimeOffset.UtcNow;
            timeToWait = (timeToWait < TimeSpan.Zero) ? TimeSpan.Zero : timeToWait;
            await Task.Delay(timeToWait, cancellationToken).ConfigureAwait(false);  // Wait until time to check
            ElectionError electionError = ElectionError.Unknown;
            try {
               // Try to acquire lease & check metadata to see if this period has been processed
               String leaseId = await blob.AcquireLeaseAsync(TimeSpan.FromSeconds(leaseDurationSeconds), null,
                  AccessCondition.GenerateIfNotModifiedSinceCondition(nextElectionTime), bro, null, CancellationToken.None).ConfigureAwait(false);
               try {
                  // Got lease: do elected work (periodically renew lease)
                  Task winnerWork = Task.Run(winnerWorker);
                  while (true) {
                     // Verify if winnerWork throws, we exit this loop & release the lease to try again
                     Task wakeUp = await Task.WhenAny(Task.Delay(TimeSpan.FromSeconds(leaseDurationSeconds - 15)), winnerWork).ConfigureAwait(false);
                     if (wakeUp == winnerWork) {   // Winner work is done
                        if (winnerWork.IsFaulted) throw winnerWork.Exception; else break;
                     }
                     await blob.RenewLeaseAsync(AccessCondition.GenerateLeaseCondition(leaseId)).ConfigureAwait(false);
                  }
                  // After work done, write to blob to indicate elected winner sucessfully did work
                  blob.UploadFromByteArray(new Byte[1], 0, 1, AccessCondition.GenerateLeaseCondition(leaseId), bro, null);
                  nextElectionTime = NextElectionTime(nextElectionTime + period, period);
               }
               finally {
                  blob.ReleaseLease(AccessCondition.GenerateLeaseCondition(leaseId));
               }
            }
            catch (StorageException ex) {
               if (ex.Matches(HttpStatusCode.Conflict, BlobErrorCodeStrings.LeaseAlreadyPresent)) electionError = ElectionError.LeaseAlreadyPresent;
               else if (ex.Matches(HttpStatusCode.PreconditionFailed)) electionError = ElectionError.ElectionOver;
               else throw;
            }
            switch (electionError) {
               case ElectionError.ElectionOver:
                  // If access condition failed, the election is over, wait until next election time
                  nextElectionTime = NextElectionTime(nextElectionTime + period, period);
                  break;
               case ElectionError.LeaseAlreadyPresent:
                  // if failed to get lease, wait a bit and retry again
                  await Task.Delay(timeBetweenLeaseRetries).ConfigureAwait(false);
                  break;
            }
         }
         // We never get here
      }

      private static DateTimeOffset NextElectionTime(DateTimeOffset marker, TimeSpan period) {
         // From marker time, figure out closest period to NOW that is in the past
         Int32 periodSeconds = (Int32)period.TotalSeconds;
         var secs = TimeSpan.FromSeconds(((Int32)(DateTimeOffset.UtcNow - marker).TotalSeconds / periodSeconds) * periodSeconds);
         return marker + secs; // Return the most recent past period to this VM immediately tries to win the election
      }
   }
}
