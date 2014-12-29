/******************************************************************************
Module:  Storage.Blob.cs
Notices: Copyright (c) by Jeffrey Richter & Wintellect
******************************************************************************/

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Blob.Protocol;
#if ResumableFileUploader
using Wintellect.Threading.AsyncProgModel;
#endif

namespace Wintellect.Azure.Storage.Blob {
   /// <summary>Defines extension methods useful when working with Azure blob containers.</summary>
   public static class BlobContainerExtensions {
      /// <summary>Creates a blob container if it doesn't exist optionally deleting all its blobs.</summary>
      /// <param name="container">The blob container to create or clear out.</param>
      /// <param name="clear">True to delete all blobs in this container.</param>
      /// <returns>The passed-in CloudBlobContainer.</returns>
      public static async Task<CloudBlobContainer> EnsureExistsAsync(this CloudBlobContainer container, Boolean clear = false) {
         if (clear) {
            await container.DeleteIfExistsAsync().ConfigureAwait(false);
            await container.RetryUntilCreatedAsync().ConfigureAwait(false);
         } else await container.CreateIfNotExistsAsync().ConfigureAwait(false);
         return container;
      }
#if DeleteMe
      public static async Task UploadDirToBlobContainerAsync(this CloudBlobContainer container, String rootPath, SearchOption searchOption = SearchOption.TopDirectoryOnly, Action<ICloudBlob> callback = null) {
         await container.EnsureExistsAsync().ConfigureAwait(false);
         foreach (var file in Directory.EnumerateFiles(rootPath, "*.*", searchOption)) {
            var cloudFile = container.GetBlockBlobReference(file.Substring(rootPath.Length + 1).Replace('\\', '/'));
            if (callback != null) callback(cloudFile);
            using (var stream = File.OpenRead(file)) {
               cloudFile.UploadFromStream(stream);
            }
         }
      }
#endif

      /// <summary>Returns True for a null result segment or if the result segment has more.</summary>
      /// <param name="crs">A ContainerResultSegment (can be null).</param>
      /// <returns>True if crs is null or if its continuation token is not null.</returns>
      public static Boolean HasMore(this ContainerResultSegment crs) {
         return (crs == null) ? true : (crs.SafeContinuationToken() != null);
      }

      /// <summary>Returns null if crs is null or if its ContinuationToken is null.</summary>
      /// <param name="crs">A ContainerResultSegment.</param>
      /// <returns>A BlobContinuationToken.</returns>
      public static BlobContinuationToken SafeContinuationToken(this ContainerResultSegment crs) {
         return (crs == null) ? null : crs.ContinuationToken;
      }

      /// <summary>Scans blobs in a container and invokes a callback method for each blob passing the specified criteria.</summary>
      /// <param name="container">The container with the blobs to scan.</param>
      /// <param name="callback">The method to invoke for each blob discovered in the container.</param>
      /// <param name="prefix">The case-sensitive prefix indicating the subset of blobs to find.</param>
      /// <param name="blobListingDetails">What additional blob data to retrieve during the scan.</param>
      /// <param name="blobRequestOptions">Allows you to control retry and timeout options.</param>
      /// <param name="operationContext">The context for the operation.</param>
      /// <returns>A Task which you can use to track the scan operation.</returns>
      public static async Task ScanBlobsAsync(this CloudBlobContainer container, Func<CloudBlockBlob, Task<Boolean>> callback, String prefix = null,
         BlobListingDetails blobListingDetails = BlobListingDetails.None, BlobRequestOptions blobRequestOptions = null, OperationContext operationContext = null) {
         prefix = container.Name + "/" + prefix;
         for (BlobResultSegment brs = null; brs.HasMore(); ) {
            brs = await container.ServiceClient.ListBlobsSegmentedAsync(prefix, true, blobListingDetails, null, brs.SafeContinuationToken(), 
               blobRequestOptions, operationContext);
            foreach (CloudBlockBlob blob in brs.Results) {
               if (await callback(blob)) return;  // Callback returns true to stop
            }
         }
      }
   }
}

namespace Wintellect.Azure.Storage.Blob {
   /// <summary>Defines extension methods useful when working with Azure blobs.</summary>
   public static class BlobExtensions {
      /// <summary>
      /// Appends a string to the end of a blob (creating the blob and appending a header if the blob doesn't exist).
      /// Before calling this method, set the blob's ContentType Property (ex: "text/csv; charset=utf-8")
      /// </summary>
      /// <param name="blob">The blob to have the string appended to.</param>
      /// <param name="string">The string to append.</param>
      /// <param name="header">The header string to put in the blob if the blob is being created.</param>
      /// <param name="maxBlockSize">The maximum block sized in bytes (0=Azure default of 4MB)</param>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      public static Task AppendAsync(this CloudBlockBlob blob, String @string, Byte[] header, Int32 maxBlockSize = 0,
         BlobRequestOptions options = null, OperationContext operationContext = null,
         CancellationToken cancellationToken = default(CancellationToken)) {
         return blob.AppendAsync(new MemoryStream(@string.Encode()), header, maxBlockSize,
            options, operationContext, cancellationToken);
      }

      /// <summary>
      /// Appends a stream's contents to the end of a blob (creating the blob and appending a header if the blob doesn't exist).
      /// Before calling this method, set the blob's ContentType Property (ex: "text/csv; charset=utf-8")
      /// </summary>
      /// <param name="blob">The blob to have the byte array appended to.</param>
      /// <param name="data">The stream with contents to append.</param>
      /// <param name="header">The header byte array to put in the blob if the blob is being created.</param>
      /// <param name="maxBlockSize">The maximum block sized in bytes (0=Azure default of 4MB)</param>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      public static async Task AppendAsync(this CloudBlockBlob blob, Stream data, Byte[] header, Int32 maxBlockSize = 0,
         BlobRequestOptions options = null, OperationContext operationContext = null,
         CancellationToken cancellationToken = default(CancellationToken)) {

         if (maxBlockSize == 0) maxBlockSize = 4 * 1024 * 1024;
         if (data.Length > maxBlockSize)
            throw new ArgumentOutOfRangeException("data", "A single data object cannot be larger than " + maxBlockSize.ToString() + " bytes.");
         if (header != null && header.Length > maxBlockSize)
            throw new ArgumentOutOfRangeException("header", "The header cannot be larger than " + maxBlockSize.ToString() + " bytes.");
         while (true) {
            using (var ms = new MemoryStream()) {
               AccessCondition accessCondition;
               IEnumerable<ListBlockItem> blockList = Enumerable.Empty<ListBlockItem>();
               try {
                  blockList = await blob.DownloadBlockListAsync(BlockListingFilter.Committed,
                     null, options, operationContext, cancellationToken).ConfigureAwait(false); // 404 if blob not found
                  accessCondition = AccessCondition.GenerateIfMatchCondition(blob.Properties.ETag); // Write if blob matches what we read
               }
               catch (StorageException se) {
                  if (!se.Matches(HttpStatusCode.NotFound, BlobErrorCodeStrings.BlobNotFound)) throw;
                  accessCondition = AccessCondition.GenerateIfNoneMatchCondition("*");  // Write if blob doesn't exist                  
               }
               try {
                  List<String> blockIds = blockList.Select(lbi => lbi.Name).ToList();
                  ListBlockItem lastBlock = blockList.LastOrDefault();
                  if (lastBlock == null) {
                     if (header != null) {
                        // No blocks exist, add header (if specified)
                        ms.Write(header, 0, header.Length);
                     }
                  } else {
                     // A block exists, can it hold the new data?
                     if (lastBlock.Length + data.Length < maxBlockSize) {
                        // Yes, download the last block's current data as long as the blob's etag hasn't changed
                        Int64 lastBlockOffset = blockList.Sum(lbi => lbi.Length) - lastBlock.Length;
                        await blob.DownloadRangeToStreamAsync(ms, lastBlockOffset, lastBlock.Length,
                           accessCondition, options, operationContext, cancellationToken).ConfigureAwait(false);  // 412 if blob modified behind our back
                        blockIds.Remove(lastBlock.Name); // Remove the block we're appending to
                     }
                  }
                  await data.CopyToAsync(ms).ConfigureAwait(false); // Append new data to end of stream
                  ms.Seek(0, SeekOrigin.Begin);
                  // Upload new block and append it to the blob
                  String newBlockId = Guid.NewGuid().ToString("N").Encode().ToBase64String();
                  blockIds.Add(newBlockId);  // Append new block to end of blob
                  await blob.PutBlockAsync(newBlockId, ms, null, accessCondition,
                     options, operationContext, cancellationToken).ConfigureAwait(false);  // PutBlock ignores access condition so this always succeeds
                  await blob.PutBlockListAsync(blockIds, accessCondition,
                     options, operationContext, cancellationToken).ConfigureAwait(false); // 409 if blob created behind our back; 400 if block ID doesn't exist (happens in another PC calls PutBlockList after our PutBlock)
                  break;   // If successful, we're done; don't retry
               }
               catch (StorageException se) {
                  // Blob got created behind our back, retry
                  if (se.Matches(HttpStatusCode.Conflict, BlobErrorCodeStrings.BlobAlreadyExists)) continue;

                  // Blob got created or modified behind our back, retry
                  if (se.Matches(HttpStatusCode.PreconditionFailed)) continue;

                  // Another PC called PutBlockList between our PutBlock & PutBlockList, 
                  // our block got destroyed, retry
                  if (se.Matches(HttpStatusCode.BadRequest, BlobErrorCodeStrings.InvalidBlockList)) continue;
                  throw;
               }
            }
         }
      }

      /// <summary>Appends a block to the end of a block blob.</summary>
      /// <param name="blob">The blob to append a block to.</param>
      /// <param name="info">Additional info used when the blob is first being created.</param>
      /// <param name="dataToAppend">The data to append as a block to the end of the blob.</param>
      /// <param name="options">The blob request options.</param>
      /// <param name="operationContext">The operation context.</param>
      /// <param name="cancellationToken">The cancellation token.</param>
      /// <returns>A Task indicating when the operation has completed.</returns>
      public static async Task AppendBlockAsync(this CloudBlockBlob blob, AppendBlobBlockInfo info,
         Byte[] dataToAppend, BlobRequestOptions options = null, OperationContext operationContext = null,
         CancellationToken cancellationToken = default(CancellationToken)) {
         blob.Properties.ContentType = info.MimeType;
         //blob.Properties.ContentDisposition = "attachment; filename=\"fname.ext\"";

         // NOTE: Put Block ignores access condition and so it always succeeds
         List<String> blockList = new List<String>();
         while (true) {
            blockList.Clear();
            AccessCondition accessCondition;
            try {
               blockList.AddRange((await blob.DownloadBlockListAsync(BlockListingFilter.Committed,
                  null, options, operationContext, cancellationToken).ConfigureAwait(false)).Select(lbi => lbi.Name)); // 404 if blob not found
               accessCondition = AccessCondition.GenerateIfMatchCondition(blob.Properties.ETag); // Write if blob matches what we read
            }
            catch (StorageException se) {
               if (!se.Matches(HttpStatusCode.NotFound, BlobErrorCodeStrings.BlobNotFound)) throw;
               accessCondition = AccessCondition.GenerateIfNoneMatchCondition("*");  // Write if blob doesn't exist                  
            }
            try {
               if (blockList.Count == 0) {   // Blob doesn't exist yet, add header (if specified)
                  // Notify client code that new blob is about to be created
                  info.OnCreatingBlob(blob);

                  if (info.Header != null) { // Write header to new blob
                     String headerBlockId = Guid.NewGuid().ToString().Encode().ToBase64String();
                     await blob.PutBlockAsync(headerBlockId, new MemoryStream(info.Header), null).ConfigureAwait(false);
                     blockList.Add(headerBlockId);
                  }
               }
               // Upload new block & add it's Id to the block list 
               String blockId = Guid.NewGuid().ToString().Encode().ToBase64String();
               await blob.PutBlockAsync(blockId, new MemoryStream(dataToAppend), null).ConfigureAwait(false);
               blockList.Add(blockId);

               // If too many blocks, remove old block (but not the header if it exists)
               var maxEntries = info.MaxEntries + ((info.Header == null) ? 0 : 1);
               if (blockList.Count > maxEntries) blockList.RemoveAt((info.Header == null) ? 0 : 1);

               // Upload the new block list
               await blob.PutBlockListAsync(blockList, AccessCondition.GenerateIfMatchCondition(blob.Properties.ETag),
                  options, operationContext, cancellationToken).ConfigureAwait(false); // 409 if blob created behind our back; 400 if block Id doesn't exist (happens in another PC calls PutBlockList after our PutBlock)
               break;   // If successful, we're done; don't retry
            }
            catch (StorageException se) {
               // Blob got created behind our back, retry
               if (se.Matches(HttpStatusCode.Conflict, BlobErrorCodeStrings.BlobAlreadyExists)) continue;

               // Blob got created or modified behind our back, retry
               if (se.Matches(HttpStatusCode.PreconditionFailed)) continue;

               // Another PC called PutBlockList between our PutBlock & PutBlockList, 
               // our block(s) got destroyed, retry
               if (se.Matches(HttpStatusCode.BadRequest, BlobErrorCodeStrings.InvalidBlockList)) continue;
               throw;
            }
         }
      }

#if DEBUG
      private static void InjectFailure(CloudBlockBlob blob) {
         String injectBlockId = Convert.ToBase64String(Guid.NewGuid().ToString("N").Encode());
         blob.PutBlock(injectBlockId, new MemoryStream("Inject create behind back".Encode()), null);
         blob.PutBlockList(new[] { injectBlockId });
      }
#endif

#if DELETEME
      public static Uri TransformUri(this ICloudBlob blob) {
         // Update with Shared Access Signature if required
         var creds = blob.ServiceClient.Credentials;
         var uri = blob.Uri;
         return creds.NeedsTransformUri ? new Uri(creds.TransformUri(uri.ToString())) : uri;
      }
      public static void MakeRestRequest(this ICloudBlob blob, Func<Uri, HttpWebRequest> requestMethod,
         Action<HttpWebRequest> processRequestMethod = null) {
         blob.MakeRestRequest(requestMethod, processRequestMethod, response => String.Empty);
      }

      public static TResult MakeRestRequest<TResult>(this ICloudBlob blob, Func<Uri, HttpWebRequest> requestMethod,
         Action<HttpWebRequest> processRequestMethod = null,
         Func<HttpWebResponse, TResult> processResponseMethod = null) {

         HttpWebRequest request = requestMethod(blob.TransformUri());
         if (processRequestMethod != null) processRequestMethod(request);
         blob.ServiceClient.Credentials.SignRequest(request);

         using (WebResponse response = request.GetResponse()) {
            return (processResponseMethod == null) ? default(TResult) : processResponseMethod((HttpWebResponse)response);
         }
      }
#endif

      /// <summary>Returns True for a null result segment or if the result segment has more.</summary>
      /// <param name="brs">A BlobResultSegment (can be null).</param>
      /// <returns>True if brs is null or if its continuation token is not null.</returns>
      public static Boolean HasMore(this BlobResultSegment brs) {
         return (brs == null) ? true : (brs.SafeContinuationToken() != null);
      }

      /// <summary>Returns null if brs is null or if its ContinuationToken is null.</summary>
      /// <param name="brs">A BlobResultSegment.</param>
      /// <returns>A BlobContinuationToken.</returns>
      public static BlobContinuationToken SafeContinuationToken(this BlobResultSegment brs) {
         return (brs == null) ? null : brs.ContinuationToken;
      }
   }

   /// <summary>An event args object passed when a new blob is created by AppendBlockAsync.</summary>
   public sealed class NewBlobEventArgs : EventArgs {
      internal NewBlobEventArgs(CloudBlockBlob blob) { Blob = blob; }
      /// <summary>Identifies the blob that is about to be created.</summary>
      public readonly CloudBlockBlob Blob;
   }

   /// <summary>Describes information required when by the AppendBlockAsync method.</summary>
   public sealed class AppendBlobBlockInfo {
      /// <summary>Indicates the Mime type to use when initially creating the blob.</summary>
      public String MimeType;
      /// <summary>Indicates the header to be written to blob when it is first created.</summary>
      public Byte[] Header = null;
      /// <summary>Indicates the maximum number of blocks the blob should have. Old blocks are deleted when going over this limit.</summary>
      public Int32 MaxEntries = 50000;

      /// <summary>Event raised when a AppendBLockAsync creates a new blob.</summary>
      public event EventHandler<NewBlobEventArgs> NewBlob;
      internal void OnCreatingBlob(CloudBlockBlob blob) {
         if (NewBlob != null) NewBlob(this, new NewBlobEventArgs(blob));
      }
   }
}

namespace Wintellect.Azure.Storage.Blob {
   /// <summary>Defines extension methods on primitive types useful when working with Azure storage.</summary>
   public static class PrimitiveTypeExtensions {
#if false
      /// <summary>Converts a timestamp into a string formatted like a blob's snapshot.</summary>
      /// <param name="dateTime">The timestamp to convert.</param>
      /// <returns>The string</returns>
      public static String ToBlobSnapshotString(this DateTimeOffset dateTime) {
         return dateTime.UtcDateTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fffffff'Z'", CultureInfo.InvariantCulture);
      }
#endif
      /// <summary>Converts an integer to an Azure blob block ID string.</summary>
      /// <param name="id">The block integer ID (0-50,000).</param>
      /// <returns>The integer as an Azure block ID.</returns>
      public static String ToBlockId(this Int32 id) {
         // Block ID's must all be the same length (hence the "D5" [for 50000]) and 
         // be Base-64 encoded to be embedded in an HTTP request
         return id.ToString("D5").Encode().ToBase64String();
      }
      /// <summary>Converts an Azure block ID back to its integer.</summary>
      /// <param name="id">The Azure blob block ID.</param>
      /// <returns>The integer.</returns>

      public static Int32 FromBlockId(this String id) {
         // Block ID's must all be the same length (hence the "D5" [for 50000]) and 
         // be Base-64 encoded to be embedded in an HTTP request
         return Int32.Parse(id.FromBase64ToBytes().Decode());
      }
   }
}

#if ResumableFileUploader
namespace Wintellect.Azure.Storage.Blob {
   public sealed class ResumableFileUploader {
      private readonly Int32 m_blockSize;
      private readonly Int32 m_concurrency;
      public Int32 BlockSize { get { return m_blockSize; } }
      public Int32 Concurrency { get { return m_concurrency; } }

      public ResumableFileUploader(Int32 blockSize, Int32 concurrency) {
         m_blockSize = blockSize;
         m_concurrency = concurrency;
      }

      public IAsyncResult BeginUpload(String pathname, CloudBlockBlob blob, Action<Int32> blockUploaded, AsyncCallback callback = null, Object state = null) {
         var ae = new AsyncEnumerator();
         return ae.BeginExecute(UploadFileInBlocks(ae, pathname, blob, blockUploaded), callback, state);
      }

      public void EndUpload(IAsyncResult asyncResult) {
         AsyncEnumerator.FromAsyncResult(asyncResult).EndExecute(asyncResult);
      }

      private IEnumerable<Int32> GetBlockNumbersFromArray(Byte[] uploadedBlocks) {
         for (Int32 blockNumber = 0; blockNumber < uploadedBlocks.Length; blockNumber++) {
            // Skip already-uploaded blocks
            if (uploadedBlocks[blockNumber] == 0) yield return blockNumber;
         }
      }

      private IEnumerator<Int32> UploadFileInBlocks(AsyncEnumerator ae, String pathname, CloudBlockBlob blob, Action<Int32> blockUploaded) {
         Int64 fileLength;
         using (var fs = new FileStream(pathname, FileMode.Open, FileAccess.Read, FileShare.Read))
         using (var mmf = MemoryMappedFile.CreateFromFile(fs, null, fileLength = fs.Length, MemoryMappedFileAccess.Read, null, HandleInheritability.None, false)) {
            Byte[] uploadedBlocks = new Byte[(fileLength - 1) / m_blockSize + 1];

            // Find out which blocks have been uploaded already?
            blob.BeginDownloadBlockList(BlockListingFilter.Uncommitted, AccessCondition.GenerateEmptyCondition(), null, null, ae.End(), null);
            yield return 1;
            try {
               foreach (ListBlockItem lbi in blob.EndDownloadBlockList(ae.DequeueAsyncResult())) {
                  Int32 blockId = lbi.Name.FromBase64ToInt32();
                  uploadedBlocks[blockId] = 1;
                  blockUploaded(blockId);
               }
            }
            catch (StorageException e) {
               if ((HttpStatusCode)e.RequestInformation.HttpStatusCode != HttpStatusCode.NotFound)
                  throw;
            }

            // Start uploading the remaining blocks:
            Int32 blocksUploading = 0;

            foreach (var blockNumber in GetBlockNumbersFromArray(uploadedBlocks)) {
               // Start uploading up to 'm_concurrency' blocks
               blocksUploading++;
               var aeBlock = new AsyncEnumerator("Block #" + blockNumber);
               aeBlock.BeginExecute(UploadBlock(aeBlock, mmf, blob, blockNumber,
                  (blockNumber == uploadedBlocks.Length - 1) ? fileLength % m_blockSize : m_blockSize), ae.End(), blockNumber);
               if (blocksUploading < m_concurrency) continue;

               // As each block completes uploading, start uploading a new block (if any are left)
               yield return 1;
               blocksUploading--;
               IAsyncResult result = ae.DequeueAsyncResult();
               AsyncEnumerator.FromAsyncResult(result).EndExecute(result);
               blockUploaded((Int32)result.AsyncState);
            }

            // Wait until all blocks have finished uploading and then commit them all
            for (; blocksUploading > 0; blocksUploading--) {
               yield return 1;
               IAsyncResult result = ae.DequeueAsyncResult();
               AsyncEnumerator.FromAsyncResult(result).EndExecute(result);
               blockUploaded((Int32)result.AsyncState);
            }

            // Commit all the blocks in order:
            blob.BeginPutBlockList(Enumerable.Range(0, uploadedBlocks.Length).Select(b => b.ToBase64()), ae.End(), null);
            yield return 1;
            blob.EndPutBlockList(ae.DequeueAsyncResult());
         }
      }

      private IEnumerator<Int32> UploadBlock(AsyncEnumerator ae, MemoryMappedFile mmf, CloudBlockBlob blob, Int32 blockNumber, Int64 length) {
         using (var stream = mmf.CreateViewStream(blockNumber * m_blockSize, length, MemoryMappedFileAccess.Read)) {
            blob.BeginPutBlock(blockNumber.ToBase64(), stream, null, ae.End(), null);
            yield return 1;
            blob.EndPutBlock(ae.DequeueAsyncResult());
         }
      }
   }
}
#endif

#if MimeSupport
#region Mime support
namespace Wintellect.Azure.Storage.Blob {
   public static class Mime {
      public static String FindMimeFromData(Byte[] data, String mimeProposed) {
         if (data == null) throw new ArgumentNullException("data");
         return FindMime(null, false, data, mimeProposed);
      }

      public static String FindMimeFromUrl(String url, Boolean urlIsFilename, string mimeProposed) {
         if (url == null) throw new ArgumentNullException("url");
         return FindMime(url, urlIsFilename, null, mimeProposed);
      }

      public static String FindMimeFromDataAndUrl(Byte[] data, String url, Boolean urlIsFilename, string mimeProposed) {
         if (data == null) throw new ArgumentNullException("data");
         if (url == null) throw new ArgumentNullException("url");
         return FindMime(url, urlIsFilename, data, mimeProposed);
      }
      private const UInt32 E_FAIL = 0x80004005;
      private const UInt32 E_INVALIDARG = 0x80000003;
      private const UInt32 E_OUTOFMEMORY = 0x80000002;

      private static String FindMime(String url, Boolean urlIsFileName, Byte[] data, String mimeProposed) {
         IntPtr outPtr = IntPtr.Zero;
         Int32 ret = FindMimeFromData(IntPtr.Zero, url, data, (data == null) ? 0 : data.Length, mimeProposed,
            MimeFlags.EnableMimeSniffing | (urlIsFileName ? MimeFlags.UrlAsFilename : MimeFlags.Default), out outPtr, 0);
         if (ret == 0 && outPtr != IntPtr.Zero) {
            String mimeRet = Marshal.PtrToStringUni(outPtr);
            Marshal.FreeHGlobal(outPtr);
            return mimeRet.ToLowerInvariant();
         }
         return null;
      }

      // http://msdn.microsoft.com/en-us/library/ms775147(VS.85).aspx
      [DllImport("UrlMon", CharSet = CharSet.Unicode, ExactSpelling = true, SetLastError = false)]
      private static extern Int32 FindMimeFromData(
         IntPtr pBC,
         [MarshalAs(UnmanagedType.LPWStr)] string pwzUrl,
         [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.I1, SizeParamIndex = 3)] byte[] pBuffer,
         Int32 cbSize,
         [MarshalAs(UnmanagedType.LPWStr)] string pwzMimeProposed,
         MimeFlags dwMimeFlags,
          out IntPtr ppwzMimeOut,
          Int32 dwReserved);

      [Flags]
      public enum MimeFlags : int {
         /// <summary>No flags specified. Use default behavior for the function.</summary>
         Default = 0,
         /// <summary>Treat the specified pwzUrl as a file name.</summary>
         UrlAsFilename = 0x00000001,
         /// <summary>
         /// Use MIME-type detection even if FEATURE_MIME_SNIFFING is detected. Usually, this feature control key would disable MIME-type detection. 
         /// Microsoft Internet Explorer 6 for Microsoft Windows XP Service Pack 2 (SP2) and later. 
         /// </summary>
         EnableMimeSniffing = 0x00000002,
         /// <summary>
         /// Perform MIME-type detection if "text/plain" is proposed, even if data sniffing is otherwise disabled. Plain text may be converted to text/html if HTML tags are detected. 
         /// Microsoft Internet Explorer 6 for Microsoft Windows XP Service Pack 2 (SP2) and later. 
         /// </summary>
         IgnoreMimeTextPlain = 0x00000004,
         /// <summary>
         /// Use the authoritative MIME type specified in pwzMimeProposed. Unless IgnoreMimeTextPlain is specified, no data sniffing is performed.
         /// Windows Internet Explorer 8.
         /// </summary>
         ServerMime = 0x00000008
      }
   }
}
#endregion
#endif