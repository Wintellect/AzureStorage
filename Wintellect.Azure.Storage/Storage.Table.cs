/******************************************************************************
Module:  Storage.Table.cs
Notices: Copyright (c) by Jeffrey Richter & Wintellect
******************************************************************************/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Wintellect.Azure.Storage.Blob;

#region Table Extension Methods
namespace Wintellect.Azure.Storage.Table {
   /// <summary>Defines Azure table class extension methods.</summary>
   public static class TableExtensions {
      /// <summary>Returns True if TableResultSegment has more data to return; that is, you should call ExecuteQuerySegmentedAsync.</summary>
      /// <param name="trs">The TableResultSegment.</param>
      /// <returns>True if you should call ExecuteQuerySegmentedAsync.</returns>
      [DebuggerStepThrough]
      public static Boolean HasMore(this TableResultSegment trs) {
         return (trs == null) ? true : (trs.SafeContinuationToken() != null);
      }


      /// <summary>Returns a TableContinuationToken you should pass to ExecuteQuerySegmentedAsync.</summary>
      /// <param name="trs">The TableResultSegment.</param>
      /// <returns>The TableContinuationToken you should pass to ExecuteQuerySegmentedAsync.</returns>
      [DebuggerStepThrough]
      public static TableContinuationToken SafeContinuationToken(this TableResultSegment trs) {
         return (trs == null) ? null : trs.ContinuationToken;
      }

      /// <summary>Converts a tick count to a 0-padded, 19-digit string to put in a table key.</summary>
      /// <param name="ticks">The number of 100-nanosecond intervals.</param>
      /// <returns>The -padded, 19-digit string.</returns>
      [DebuggerStepThrough]
      public static String TicksToString(this Int64 ticks) {
         return ticks.ToString("D19"); // 0-padded, 19-digit [DateTime.MaxValue.Ticks.ToString().Length is 19]
      }

#if false
      [DebuggerStepThrough]
      private static String Invert(this String @string, Int32 length) {
         var s = new Char[length];
         for (Int32 n = 0; n < s.Length; n++)
            if (n < @string.Length) s[n] = (Char)(Char.MaxValue - @string[n]);
            else s[n] = (Char)(Char.MaxValue - ' ');
         return new String(s);
      }

      [DebuggerStepThrough]
      private static String NextKey(this String @string) {
         var sb = new StringBuilder(@string);
         sb[sb.Length - 1]++;
         return sb.ToString();
      }

      [DebuggerStepThrough]
      private static String PreviousKey(this String @string) {
         var sb = new StringBuilder(@string);
         sb[sb.Length - 1]--;
         return sb.ToString();
      }
#endif
   }
}
#endregion

#region AzureTable
namespace Wintellect.Azure.Storage.Table {
   /// <summary>A light-weight struct wrapping a CloudTable. The methods exposed force the creation of scalable services.</summary>
   public struct AzureTable {
      #region Static members
      /// <summary>The maximum number of bytes supported by a byte array or String property.</summary>
      public const Int32 MaxPropertyBytes = 65536;

      /// <summary>Returns the minimum DateTimeOffset value supported by an Azure table.</summary>
      public static DateTimeOffset MinDateTime { get { return Microsoft.WindowsAzure.Storage.Table.Protocol.TableConstants.MinDateTime; } }

      /// <summary>Returns False if the passed string value contains any characters not support by an Azure table key.</summary>
      /// <param name="key">The string to be used as an Azure table partition or row key.</param>
      /// <returns>False if the string has at least 1 invalid character; else True.</returns>
      public static Boolean IsValidKey(String key) {
         // http://msdn.microsoft.com/en-us/library/windowsazure/dd179338.aspx
         if (key.Length > 512) return false;  // Key can be up to 1KB in size
         const String c_invalidKeyChars = @"#/?\";
         foreach (Char c in key) {
            if (c_invalidKeyChars.IndexOf(c) != -1) return false;
            // Characters forbidden by XML 1.0 
            if (0x00 <= c && c <= 0x1f) return false;
            if (0x7f <= c && c <= 0x9f) return false;
         }
         return true;
      }

      /// <summary>Converts a date/time to a valid Azure table entity ETag value.</summary>
      /// <param name="timestamp">The date/time value.</param>
      /// <returns>An ETag representation of the date/time value.</returns>
      public static String ToETag(DateTimeOffset timestamp) {
         // Example: "W/\"datetime'2012-09-02T19%3A02%3A42.837Z'\""
         return String.Format("W/\"datetime'{0}'\"", timestamp.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFK")).Replace(":", "%3A");
      }

      /// <summary>Executes a function repeatedly if it fails with PreconditionFailed (412).</summary>
      /// <param name="operation">The function to try repeatedly if it fails with PreconditionFailed.</param>
      /// <param name="maxRetries">The maximum number of times to retry the function.</param>
      /// <returns>A Task indicating when the function succeeds or if it has been executed maxRetries times.</returns>
      public static async Task OptimisticRetryAsync(Func<Task> operation, Int32 maxRetries = -1) {
         for (Int32 retry = 0; ; retry++) {
            try {
               await operation().ConfigureAwait(false);
               return;
            }
            catch (StorageException se) {
               // If optimistic concurrency failed, it's OK, try again
               if (!se.Matches(HttpStatusCode.PreconditionFailed)) throw;
               if (maxRetries >= 0 && retry >= maxRetries) throw;
            }
         }
      }

      /// <summary>Executes a function repeatedly if it fails with PreconditionFailed (412).</summary>
      /// <typeparam name="TResult">The function's return type.</typeparam>
      /// <param name="operation">The function to try repeatedly if it fails with PreconditionFailed.</param>
      /// <param name="maxRetries">The maximum number of times to retry the function.</param>
      /// <returns>A Task indicating when the function succeeds or if it has been executed maxRetries times.</returns>
      /// <returns>The function's return value.</returns>
      public static async Task<TResult> OptimisticRetryAsync<TResult>(Func<Task<TResult>> operation, Int32 maxRetries = -1) {
         for (Int32 retry = 0; ; retry++) {
            try {
               return await operation().ConfigureAwait(false);
            }
            catch (StorageException se) {
               // If optimistic concurrency failed, it's OK, try again
               if (!se.Matches(HttpStatusCode.PreconditionFailed)) throw;
               if (maxRetries >= 0 && retry >= maxRetries) throw;
            }
         }
      }

      /// <summary>Implicitly converts an AzureTable instance to a CloudTable reference.</summary>
      /// <param name="at">The AzureTable instance.</param>
      /// <returns>The wrapped CloudTable object reference.</returns>
      public static implicit operator CloudTable(AzureTable at) { return at.CloudTable; }

      /// <summary>Implicitly converts a CloudTable reference to an AzureTable instance.</summary>
      /// <returns>A reference to a CloudTable object.</returns>
      /// <param name="ct">A reference to the CloudTable object.</param>
      /// <returns>An AzureTable instance wrapping the CloudTable object.</returns>
      public static implicit operator AzureTable(CloudTable ct) { return new AzureTable(ct); }
      #endregion

      // The wrapped CloudTable reference.
      private readonly CloudTable CloudTable;

      /// <summary>Initializes a new AzureTable instance from a CloudTable reference</summary>
      /// <param name="cloudTable">The CloudTable reference that this structure wraps.</param>
      public AzureTable(CloudTable cloudTable) { CloudTable = cloudTable; }

      /// <summary>Gets the table's name.</summary>
      public String Name { get { return CloudTable.Name; } }

      /// <summary>Gets the table's CloudTableClient.</summary>
      public CloudTableClient ServiceClient { get { return CloudTable.ServiceClient; } }

      /// <summary>Gets the URI that identifies the table.</summary>
      public Uri Uri { get { return CloudTable.Uri; } }

      /// <summary>Ensures the table exists. Optionally deleting it first to clear it out.</summary>
      /// <param name="clear">Pass true to delete the table first.</param>
      /// <returns>The same AzureTable instance for fluent programming pattern.</returns>
      [DoesServiceRequest, MethodImpl(MethodImplOptions.AggressiveInlining)]
      public async Task<AzureTable> EnsureExistsAsync(Boolean clear = false) {
         if (clear) {
            await DeleteIfExistsAsync().ConfigureAwait(false);
            await this.RetryUntilCreatedAsync().ConfigureAwait(false);
         } else await CreateIfNotExistsAsync().ConfigureAwait(false);
         return this;
      }

      /// <summary>
      /// Returns a task that performs an asynchronous operation to create a table.
      /// </summary>
      /// <param name="requestOptions">A Microsoft.WindowsAzure.Storage.Table.TableRequestOptions object that specifies execution options, such as retry policy and timeout settings, for the operation.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest, MethodImpl(MethodImplOptions.AggressiveInlining)]
      public Task CreateAsync(TableRequestOptions requestOptions = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudTable.CreateAsync(requestOptions, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous operation to create a table if it does not already exist.
      /// </summary>
      /// <param name="requestOptions">A Microsoft.WindowsAzure.Storage.Table.TableRequestOptions object that specifies execution options, such as retry policy and timeout settings, for the operation.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>True if the table was created; false if the table already existed.</returns>
      [DoesServiceRequest, MethodImpl(MethodImplOptions.AggressiveInlining)]
      public Task<Boolean> CreateIfNotExistsAsync(TableRequestOptions requestOptions = null, OperationContext operationContext = null,
         CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudTable.CreateIfNotExistsAsync(requestOptions, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous operation to delete a table.
      /// </summary>
      /// <param name="requestOptions">A Microsoft.WindowsAzure.Storage.Table.TableRequestOptions object that specifies execution options, such as retry policy and timeout settings, for the operation.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest, MethodImpl(MethodImplOptions.AggressiveInlining)]
      public Task DeleteAsync(TableRequestOptions requestOptions = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudTable.DeleteAsync(requestOptions, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous operation to delete the table if it exists.
      /// </summary>
      /// <param name="requestOptions">A Microsoft.WindowsAzure.Storage.Table.TableRequestOptions object that specifies execution options, such as retry policy and timeout settings, for the operation.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A Task object that represents the current operation.</returns>
      [DoesServiceRequest, MethodImpl(MethodImplOptions.AggressiveInlining)]
      public Task<Boolean> DeleteIfExistsAsync(TableRequestOptions requestOptions = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudTable.DeleteIfExistsAsync(requestOptions, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous table operation using the specified Microsoft.WindowsAzure.Storage.Table.TableRequestOptions and Microsoft.WindowsAzure.Storage.OperationContext.
      /// </summary>
      /// <param name="operation">A Microsoft.WindowsAzure.Storage.Table.TableOperation object that represents the operation to perform.</param>
      /// <param name="requestOptions">A Microsoft.WindowsAzure.Storage.Table.TableRequestOptions object that specifies execution options, such as retry policy and timeout settings, for the operation.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object for tracking the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A Task object that represents the current operation.</returns>
      [DoesServiceRequest, MethodImpl(MethodImplOptions.AggressiveInlining)]
      public Task<TableResult> ExecuteAsync(TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudTable.ExecuteAsync(operation, requestOptions, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous operation to execute a batch of operations on a table, using the specified Microsoft.WindowsAzure.Storage.Table.TableRequestOptions and Microsoft.WindowsAzure.Storage.OperationContext.
      /// </summary>
      /// <param name="batch">The Microsoft.WindowsAzure.Storage.Table.TableBatchOperation object representing the operations to execute on the table.</param>
      /// <param name="requestOptions">A Microsoft.WindowsAzure.Storage.Table.TableRequestOptions object that specifies execution options, such as retry policy and timeout settings, for the operation.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A Task object that represents the current operation.</returns>
      [DoesServiceRequest, MethodImpl(MethodImplOptions.AggressiveInlining)]
      public Task<IList<TableResult>> ExecuteBatchAsync(TableBatchOperation batch, TableRequestOptions requestOptions = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudTable.ExecuteBatchAsync(batch, requestOptions, operationContext, cancellationToken);
      }

      /// <summary>Creates a TableQueryChunk object allowing you to query the table</summary>
      /// <param name="query">A Microsoft.WindowsAzure.Storage.Table.TableQuery representing the query to execute.</param>
      /// <returns>A Task object that represents the current operation.</returns>
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public TableQueryChunk CreateQueryChunker(TableQuery query = null) { return new TableQueryChunk(this, query); }


      /// <summary>
      /// Returns a task that performs an asynchronous request to get the permissions settings for the table.
      /// </summary>
      /// <param name="requestOptions">A Microsoft.WindowsAzure.Storage.Table.TableRequestOptions object that specifies execution options, such as retry policy and timeout settings, for the operation.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A Task object that represents the current operation.</returns>
      [DoesServiceRequest, MethodImpl(MethodImplOptions.AggressiveInlining)]
      public Task<TablePermissions> GetPermissionsAsync(TableRequestOptions requestOptions = null, OperationContext operationContext = null,
         CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudTable.GetPermissionsAsync(requestOptions, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a shared access signature for the table.
      /// </summary>
      /// <param name="policy">The access policy for the shared access signature.</param>
      /// <param name="accessPolicyIdentifier">An access policy identifier.</param>
      /// <param name="startPartitionKey">The start partition key, or null.</param>
      /// <param name="startRowKey">The start row key, or null.</param>
      /// <param name="endPartitionKey">The end partition key, or null.</param>
      /// <param name="endRowKey">The end row key, or null.</param>
      /// <returns>A shared access signature, as a URI query string.</returns>
      /// <remarks>The query string returned includes the leading question mark.</remarks>
      // Exceptions:
      //   System.InvalidOperationException:
      //     Thrown if the current credentials don't support creating a shared access signature.
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public String GetSharedAccessSignature(SharedAccessTablePolicy policy, String accessPolicyIdentifier, String startPartitionKey, String startRowKey,
         String endPartitionKey, String endRowKey) {
         return CloudTable.GetSharedAccessSignature(policy, accessPolicyIdentifier, startPartitionKey, startRowKey, endPartitionKey, endRowKey);
      }

      /// <summary>Returns the name of the table.</summary>
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public override String ToString() { return CloudTable.ToString(); }
      /// <summary>Determines whether the specified System.Object is equal to the current System.Object.</summary>
      /// <param name="obj">The object to compare with the current object.</param>
      /// <returns>true if the specified object is equal to the current object; otherwise, false.</returns>
      public override Boolean Equals(Object obj) { return CloudTable.Equals(obj); }
      /// <summary>Serves as a hash function for a particular type.</summary>
      /// <returns>A hash code for the current AzureTable.</returns>
      public override Int32 GetHashCode() { return CloudTable.GetHashCode(); }

      #region Table -> Blob Backup & Restore
      private const Int32 MaxBlobBlockSize = 4 * 1024 * 1024;
      private const UInt16 BackupRestoreVersion = 1;

      /// <summary>An object with statics about a table copy operation.</summary>
      public sealed class CopyTableStats {
         internal CopyTableStats() { }
         /// <summary>The number of entities copied.</summary>
         public Int32 NumEntitiesCopied { get; private set; }
         /// <summary>The timestamp of the entity modified least recently.</summary>
         public DateTimeOffset MinimumTimestamp { get; private set; }
         /// <summary>The timestamp of the entity modified most recently.</summary>
         public DateTimeOffset MaximumTimestamp { get; private set; }
         internal void Update(DynamicTableEntity dte) {
            if (NumEntitiesCopied++ == 0) {
               // First entity, initialize the minimum & maximum timestamps
               MinimumTimestamp = MaximumTimestamp = dte.Timestamp;
            } else {
               // Not the 1st entity, update the minimum & maximum timestamps
               if (MinimumTimestamp > dte.Timestamp) MinimumTimestamp = dte.Timestamp;
               if (MaximumTimestamp < dte.Timestamp) MaximumTimestamp = dte.Timestamp;
            }
         }
      }

      /// <summary>Copies a table's entities to a blob.</summary>
      /// <param name="blob">The blob to get the table's entities.</param>
      /// <param name="query">The query to apply to the table when extracting entities.</param>
      /// <param name="predicate">A method called for each entity to filter out entities to copy.</param>
      /// <param name="requestOptions">A Microsoft.WindowsAzure.Storage.Table.TableRequestOptions object that specifies execution options, such as retry policy and timeout settings, for the operation.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A Task wrapping the resulting statistics of the copy operation.</returns>
      public async Task<CopyTableStats> CopyToBlobAsync(CloudBlockBlob blob, TableQuery query = null, Func<DynamicTableEntity, Boolean> predicate = null,
         TableRequestOptions requestOptions = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         CopyTableStats stats = new CopyTableStats();
         BinaryWriter writer = new BinaryWriter(new MemoryStream());
         writer.Write(BackupRestoreVersion);
         Int32 blockId = 0;
         for (var tqc = CreateQueryChunker(query); tqc.HasMore; ) {
            var chunk = await tqc.TakeAsync(predicate, requestOptions, operationContext, cancellationToken).ConfigureAwait(false);
            foreach (DynamicTableEntity dte in chunk) {
               stats.Update(dte);
               Int64 length = writer.BaseStream.Position;
               WriteDynamicTableEntity(writer, dte);
               if (writer.BaseStream.Length > MaxBlobBlockSize) {  // We're over the max block size
                  writer.BaseStream.SetLength(length);   // Remove most recently added DTE from stream to get below 4MB
                  writer.BaseStream.Seek(0, SeekOrigin.Begin);
                  await blob.PutBlockAsync(blockId.ToBlockId(), writer.BaseStream, null).ConfigureAwait(false);  // Put the block
                  blockId++;
                  // Clear the stream & re-write the last DTE into it
                  writer.BaseStream.SetLength(0);
                  WriteDynamicTableEntity(writer, dte);
               }
            }
         }
         // Force write of last block & then put block list
         if (writer.BaseStream.Length > 0) {
            writer.BaseStream.Seek(0, SeekOrigin.Begin);
            await blob.PutBlockAsync(blockId.ToBlockId(), writer.BaseStream, null).ConfigureAwait(false);
            blockId++;
         }
         await blob.PutBlockListAsync(Enumerable.Range(0, blockId).Select(id => id.ToBlockId())).ConfigureAwait(false);
         return stats;
      }

      private static void WriteDynamicTableEntity(BinaryWriter writer, DynamicTableEntity dte) {
         var startOffset = writer.BaseStream.Position;
         writer.Write(0);  // 4-byte length (corrected later)

         writer.Write(dte.PartitionKey);
         writer.Write(dte.RowKey);
         writer.Write(dte.Timestamp.Ticks);
         writer.Write((Byte)dte.Properties.Count);   // Max properties a table entity can hold is 252
         foreach (var prop in dte.Properties) {
            writer.Write((Byte)prop.Value.PropertyType);
            writer.Write(prop.Key);
            switch (prop.Value.PropertyType) {
               case EdmType.Boolean: writer.Write(prop.Value.BooleanValue.Value); break;
               case EdmType.Int32: writer.Write(prop.Value.Int32Value.Value); break;
               case EdmType.Int64: writer.Write(prop.Value.Int64Value.Value); break;
               case EdmType.Double: writer.Write(prop.Value.DoubleValue.Value); break;
               case EdmType.Guid: writer.Write(prop.Value.GuidValue.Value); break;
               case EdmType.DateTime: writer.Write(prop.Value.DateTimeOffsetValue.Value.Ticks); break;
               case EdmType.Binary: writer.Write(prop.Value.BinaryValue.Length); writer.Write(prop.Value.BinaryValue); break;
               case EdmType.String:
                  Byte[] bytes = prop.Value.StringValue.Encode();
                  writer.Write(bytes.Length); writer.Write(bytes); break;
            }
         }
         // Update the length with the actual length & position after the DTE
         var endOffset = writer.BaseStream.Position;
         writer.BaseStream.Seek(startOffset, SeekOrigin.Begin);
         writer.Write((Int32)(endOffset - startOffset));
         writer.Seek(0, SeekOrigin.End);
      }

      /// <summary>Copies entities saved in a blob back to the table's entities.</summary>
      /// <param name="blob">The blob containing the entities.</param>
      /// <param name="tableOperationType">The type of operation to try when applying the blob's entity to the table.</param>
      /// <param name="predicate">A method called for each entity to filter out entities to copy.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A Task wrapping the resulting statistics of the copy operation.</returns>
      public async Task<CopyTableStats> CopyFromBlobAsync(CloudBlockBlob blob, TableOperationType tableOperationType,
         Func<DynamicTableEntity, Boolean> predicate = null, CancellationToken cancellationToken = default(CancellationToken)) {
         CopyTableStats stats = new CopyTableStats();
         TableBatchOperation tbo = new TableBatchOperation();
         String lastPartitionKey = null;
         BinaryReader reader = new BinaryReader(new MemoryStream());

         await blob.DownloadRangeToStreamAsync(reader.BaseStream, 0, sizeof(UInt16), cancellationToken).ConfigureAwait(false);
         Int64 blobLength = blob.Properties.Length;
         reader.BaseStream.Seek(0, SeekOrigin.Begin);
         UInt16 version = reader.ReadUInt16();
         Debug.Assert(version == BackupRestoreVersion);
         Int64 blobOffset = 0;

         while (true) {
            Int64 streamOffset = reader.BaseStream.Position;
            DynamicTableEntity dte = ReadDynamicTableEntity(version, reader);
            if (dte == null) {
               // The stream doesn't contain a full DTE; read another chunk starting after last full DTE
               blobOffset += streamOffset;
               reader.BaseStream.SetLength(0);
               Int64 numBytes = Math.Min(MaxBlobBlockSize, blobLength - blobOffset);
               if (numBytes == 0) break; // Reached end of blob

               await blob.DownloadRangeToStreamAsync(reader.BaseStream, blobOffset, numBytes, cancellationToken).ConfigureAwait(false);
               reader.BaseStream.Seek(0, SeekOrigin.Begin);
               continue;   // Try again
            }
            if (predicate != null) if (!predicate(dte)) continue;
            TableOperation op = null;
            switch (tableOperationType) {
               case TableOperationType.Insert: op = TableOperation.Insert(dte); break;
               case TableOperationType.InsertOrMerge: op = TableOperation.InsertOrMerge(dte); break;
               case TableOperationType.InsertOrReplace: op = TableOperation.InsertOrReplace(dte); break;
               case TableOperationType.Merge: op = TableOperation.Merge(dte); break;
               case TableOperationType.Delete: op = TableOperation.Delete(dte); break;
               case TableOperationType.Replace: op = TableOperation.Replace(dte); break;
               case TableOperationType.Retrieve: throw new NotSupportedException("Retrieve operation not supported");
            }

            // If this entity can't go in the batch, execute the batch
            if (tbo.Count == 100 || (tbo.Count > 0 && lastPartitionKey != dte.PartitionKey)) {
               await ExecuteBatchSafelyAsync(tbo, null, null, cancellationToken).ConfigureAwait(false);
               tbo.Clear();
               lastPartitionKey = null;
            }
            tbo.Add(op);
            stats.Update(dte);
            lastPartitionKey = dte.PartitionKey;
         }
         if (tbo.Count > 0)
            await ExecuteBatchSafelyAsync(tbo, null, null, cancellationToken).ConfigureAwait(false);
         return stats;
      }

      private async Task<IList<TableResult>> ExecuteBatchSafelyAsync(TableBatchOperation batch,
         TableRequestOptions requestOptions = null, OperationContext operationContext = null,
         CancellationToken cancellationToken = default(CancellationToken)) {
         try {
            return await ExecuteBatchAsync(batch, requestOptions, operationContext, cancellationToken).ConfigureAwait(false);
         }
         catch (StorageException ex) {
            if (!ex.Matches(HttpStatusCode.BadRequest, "ContentLengthExceeded")) throw;
         }
         // The batch was too big, batching 4 at a time will work (4 * 1MB <= 4MB)
         // TODO: we could be smarter here to improve performance
         var results = new List<TableResult>(batch.Count);
         List<Task<IList<TableResult>>> tasks = new List<Task<IList<TableResult>>>(batch.Count / 4 + 1);
         var batchToAttempt = new TableBatchOperation();
         for (Int32 i = 0; i < batch.Count / 4; i++) {
            batchToAttempt.Clear();
            Int32 numberDone = i * 4;
            for (Int32 op = 0; op < Math.Min(4, batch.Count - numberDone); op++)
               batchToAttempt.Add(batch[numberDone + op]);
            tasks.Add(ExecuteBatchAsync(batchToAttempt));
         }
         await Task.WhenAll(tasks).ConfigureAwait(false);
         foreach (Task<IList<TableResult>> t in tasks) results.AddRange(await t.ConfigureAwait(false));
         return results;
      }

      private static DynamicTableEntity ReadDynamicTableEntity(UInt16 version, BinaryReader reader) {
         if (reader.BaseStream.Position + sizeof(Int32) > reader.BaseStream.Length) return null;
         var dteLength = reader.ReadInt32();
         if (reader.BaseStream.Position + dteLength - sizeof(Int32) > reader.BaseStream.Length) return null;

         // We have a complete DTE in the stream; read it.
         String partitionKey = reader.ReadString();
         String rowKey = reader.ReadString();
         DynamicTableEntity dte = new DynamicTableEntity(partitionKey, rowKey) {
            Timestamp = new DateTimeOffset(reader.ReadInt64(), TimeSpan.Zero),
            ETag = "*"
         };

         Byte numProps = reader.ReadByte();
         for (Int32 prop = 0; prop < numProps; prop++) {
            EdmType edmType = (EdmType)reader.ReadByte();
            String propName = reader.ReadString();
            EntityProperty ep;
            switch (edmType) {
               case EdmType.Boolean: ep = new EntityProperty(reader.ReadBoolean()); break;
               case EdmType.Int32: ep = new EntityProperty(reader.ReadInt32()); break;
               case EdmType.Int64: ep = new EntityProperty(reader.ReadInt64()); break;
               case EdmType.Double: ep = new EntityProperty(reader.ReadDouble()); break;
               case EdmType.Guid: ep = new EntityProperty(reader.ReadGuid()); break;
               case EdmType.DateTime: ep = new EntityProperty(new DateTimeOffset(reader.ReadInt64(), TimeSpan.Zero)); break;
               case EdmType.Binary: ep = new EntityProperty(reader.ReadBytes(reader.ReadInt32())); break;
               case EdmType.String:
                  ep = new EntityProperty(reader.ReadBytes(reader.ReadInt32()).Decode()); break;
               default: throw new NotSupportedException("Unknown EDM type");
            }
            dte.Properties.Add(propName, ep);
         }
         return dte;
      }

      /// <summary>Copies the table's entities to another table.</summary>
      /// <param name="destinationTable">The table to receive this table's entities.</param>
      /// <param name="query">The query to apply to the table when extracting entities.</param>
      /// <param name="tableOperationType">The type of operation to try when applying this table's entity to the destination table.</param>
      /// <param name="predicate">A method called for each entity to filter out entities to copy.</param>
      /// <param name="requestOptions">A Microsoft.WindowsAzure.Storage.Table.TableRequestOptions object that specifies execution options, such as retry policy and timeout settings, for the operation.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A Task wrapping the resulting statistics of the copy operation.</returns>
      public async Task<CopyTableStats> CopyToTableAsync(AzureTable destinationTable, TableOperationType tableOperationType,
         TableQuery query = null,
         Func<DynamicTableEntity, Boolean> predicate = null,
         TableRequestOptions requestOptions = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {

         CopyTableStats stats = new CopyTableStats();
         TableBatchOperation tbo = new TableBatchOperation();
         String lastPartitionKey = null;
         for (var tqc = CreateQueryChunker(query); tqc.HasMore; ) {
            var chunk = await tqc.TakeAsync(predicate, requestOptions, operationContext, cancellationToken).ConfigureAwait(false);
            foreach (DynamicTableEntity dte in chunk) {
               TableOperation op = null;
               switch (tableOperationType) {
                  case TableOperationType.Insert: op = TableOperation.Insert(dte); break;
                  case TableOperationType.InsertOrMerge: op = TableOperation.InsertOrMerge(dte); break;
                  case TableOperationType.InsertOrReplace: op = TableOperation.InsertOrReplace(dte); break;
                  case TableOperationType.Merge: op = TableOperation.Merge(dte); break;
                  case TableOperationType.Delete: op = TableOperation.Delete(dte); break;
                  case TableOperationType.Replace: op = TableOperation.Replace(dte); break;
                  case TableOperationType.Retrieve: throw new NotSupportedException("Retrieve operation not supported");
               }

               // If this entity can't go in the batch, execute the batch
               if (tbo.Count == 100 || (tbo.Count > 0 && lastPartitionKey != dte.PartitionKey)) {
                  await destinationTable.ExecuteBatchSafelyAsync(tbo, requestOptions, operationContext, cancellationToken).ConfigureAwait(false);
                  tbo.Clear();
                  lastPartitionKey = null;
               }
               tbo.Add(op);
               lastPartitionKey = dte.PartitionKey;
               stats.Update(dte);
            }
         }
         // Force write of last entities
         if (tbo.Count > 0)
            await destinationTable.ExecuteBatchSafelyAsync(tbo, null, null, cancellationToken).ConfigureAwait(false);
         return stats;
      }
      #endregion
   }
}
#endregion

#region TableFilterBuilder
#if TEST
namespace Wintellect.Azure.Storage.Table.Test {
   using Wintellect.Azure.Storage.Table;
   public static class Test {
      public static void Main() {
         var ss = new PropertyName<Customer>()[c => c.Boolean];
         TableFilterBuilder<Customer> fe = new TableFilterBuilder<Customer>();
         var fe0 = fe.And(c => c.Name, "Jeff", RangeOp.StartsWith);

         var fe1 = fe.And(c => c.Name, CompareOp.EQ, "Jeff").Or(c => c.Name, CompareOp.EQ, "Ron");
         var fe2 = fe.And(c => c.Boolean, EqualOp.NE, true)
            .And(c => c.Count32, CompareOp.EQ, 1)
            .And(c => c.Count64, CompareOp.EQ, 2L)
            .And(c => c.CountSingle, CompareOp.NE, 4F)
            .And(c => c.CountDouble, CompareOp.EQ, 13D)
            .And(fe.And(c => c.Guid, EqualOp.EQ, Guid.NewGuid()).And(c => c.Name, CompareOp.EQ, "Jeff").Or(c => c.Binary, EqualOp.EQ, new Byte[] { 1, 2, 3, 4 }));

         var fe3 = fe.And(c => c.Boolean, EqualOp.NE, false)
            .And(c => c.Count32, CompareOp.EQ, 32)
            .And(c => c.Count64, CompareOp.EQ, 64)
            .And(c => c.CountSingle, CompareOp.NE, 1.0)
            .And(c => c.CountDouble, CompareOp.EQ, 2.0)
            .And(fe.And(c => c.Guid, EqualOp.EQ, Guid.Empty).And(c => c.Name, CompareOp.EQ, "Jeff").Or(c => c.Binary, EqualOp.EQ, new Byte[] { 1 }));

         var s = fe2.And(fe1).Or(c => c.Birthday, CompareOp.LE, DateTimeOffset.Now);
         Console.Write(s);
      }

      public sealed class Customer {
         public Boolean Boolean { get; set; }
         public Int32 Count32 { get; set; }
         public Int64 Count64 { get; set; }
         public Single CountSingle { get; set; }
         public Double CountDouble { get; set; }
         public Guid Guid { get; set; }
         public DateTimeOffset? Birthday { get; set; }
         public String Name { get; set; }
         public Byte[] Binary { get; set; }
      }
   }
}
#endif
namespace Wintellect.Azure.Storage.Table {
   /// <summary>Used to perform an equals (EQ) or not equals (NE) comparison.</summary>
   public enum EqualOp {
      /// <summary>Equal to.</summary>
      EQ,
      /// <summary>Not equal to.</summary>
      NE
   }
   /// <summary>Used to perform an equals (EQ), not equals (NE), greater-than (GT), 
   /// greater-than-or-equals to (GE), less-than (LT), or less-than-or-equals to (LE) comparison.</summary>
   public enum CompareOp {
      /// <summary>Equal to.</summary>
      EQ = EqualOp.EQ,
      /// <summary>Not equal to.</summary>
      NE = EqualOp.NE,
      /// <summary>Greater than.</summary>
      GT,
      /// <summary>Greater than or equal to.</summary>
      GE,
      /// <summary>Less than.</summary>
      LT,
      /// <summary>Less than or equal to.</summary>
      LE
   }
   /// <summary>Used to perform an range comparison.</summary>
   public enum RangeOp {
      /// <summary>Equivalent of GE and LT.</summary>
      IncludeLowerExcludeUpper = (CompareOp.GE << 8) | CompareOp.LT,
      /// <summary>Equivalent of GE and LT.</summary>
      StartsWith = IncludeLowerExcludeUpper,
      /// <summary>Equivalent of GE and LE.</summary>
      IncludeLowerIncludeUpper = (CompareOp.GE << 8) | CompareOp.LE,
      /// <summary>Equivalent of GT and LT.</summary>
      ExcludeLowerExcludeUpper = (CompareOp.GT << 8) | CompareOp.LT,
      /// <summary>Equivalent of GE and LE.</summary>
      ExcludeLowerIncludeUpper = (CompareOp.GT << 8) | CompareOp.LE
   }

   /// <summary>Use this light-weight struct to help you create a table query filter string.</summary>
   /// <typeparam name="TEntity">The whose properties you want to include in the filter string.</typeparam>
   public struct TableFilterBuilder<TEntity> {
      private static PropertyName<TEntity> s_propName = new PropertyName<TEntity>();
      private readonly String m_filter;
      private TableFilterBuilder(String filter) { m_filter = filter; }
      /// <summary>The string to be assigned to TableQuery's FilterString property.</summary>
      public override String ToString() { return m_filter; }

      /// <summary>Implicitly converts a TableFilterBuilder to a String (for TableQuery's FilterString property).</summary>
      /// <param name="fe">The TableFilterBuilder to convert.</param>
      /// <returns>The string to be assigned to TableQuery's FilterString property.</returns>
      public static implicit operator String(TableFilterBuilder<TEntity> fe) { return fe.ToString(); }
      private String BuildPropertyExpr(LambdaExpression property, CompareOp compareOp, String value) {
         if (value == null) throw new ArgumentNullException("value");

         const String ops = "eqnegtgeltle";  // Must be in same order as CompareOps enum
         String expr = String.Format("({0} {1} {2})", s_propName[property], ops.Substring(2 * (Int32)compareOp, 2), value);
         return expr;
      }

      private TableFilterBuilder<TEntity> Conjunction(String expr, [CallerMemberName] String conjunction = null) {
         if (this.m_filter != null) { // If previous expression, apply conjunction
            conjunction = conjunction == "And" ? " and " : " or ";
            expr = new StringBuilder("(").Append(this.m_filter).Append(conjunction).Append(expr).Append(")").ToString();
         }
         return new TableFilterBuilder<TEntity>(expr);
      }

      private static String NextStringKey(String @string) {
         return @string.Substring(0, @string.Length - 1)
            + ((Char)(@string[@string.Length - 1] + 1)).ToString();
      }
      #region Value formatters
      private String Format(Boolean value) { return value ? "true" : "false"; }
      private String Format(DateTimeOffset value) {
         return String.Format("datetime'{0}'", value.UtcDateTime.ToString("o"));
      }
      private String Format(Guid value) {
         return String.Format("guid'{0}'", value.ToString());
      }
      private String Format(Byte[] value) {
         var sb = new StringBuilder(value.Length * 2);
         foreach (Byte b in value) sb.Append(b.ToString("x2"));
         return String.Format("X'{0}'", sb);
      }
      #endregion

      #region And Members
      /// <summary>Ands a TableFilterBuilder expression with another.</summary>
      /// <param name="other">The other TableFilterBuidler expression.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> And(TableFilterBuilder<TEntity> other) {
         return Conjunction(other);
      }

      /// <summary>Ands the current expression with a new Boolean expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="equalOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> And(Expression<Func<TEntity, Boolean>> property, EqualOp equalOp, Boolean value) {
         return Conjunction(BuildPropertyExpr(property, (CompareOp)equalOp, Format(value)));
      }

      /// <summary>Ands the current expression with a new Boolean expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="equalOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> And(Expression<Func<TEntity, Boolean?>> property, EqualOp equalOp, Boolean value) {
         return Conjunction(BuildPropertyExpr(property, (CompareOp)equalOp, Format(value)));
      }

      /// <summary>Ands the current expression with a new Int32 expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="compareOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> And(Expression<Func<TEntity, Int32>> property, CompareOp compareOp, Int32 value) {
         return Conjunction(BuildPropertyExpr(property, compareOp, value.ToString()));
      }

      /// <summary>Ands the current expression with a new Int32 expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="compareOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> And(Expression<Func<TEntity, Int32?>> property, CompareOp compareOp, Int32 value) {
         return Conjunction(BuildPropertyExpr(property, compareOp, value.ToString()));
      }

      /// <summary>Ands the current expression with a new Int64 expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="compareOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> And(Expression<Func<TEntity, Int64>> property, CompareOp compareOp, Int64 value) {
         return Conjunction(BuildPropertyExpr(property, compareOp, value.ToString()));
      }

      /// <summary>Ands the current expression with a new Int64 expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="compareOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> And(Expression<Func<TEntity, Int64?>> property, CompareOp compareOp, Int64 value) {
         return Conjunction(BuildPropertyExpr(property, compareOp, value.ToString()));
      }

      /// <summary>Ands the current expression with a new Double expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="compareOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> And(Expression<Func<TEntity, Double>> property, CompareOp compareOp, Double value) {
         return Conjunction(BuildPropertyExpr(property, compareOp, value.ToString()));
      }

      /// <summary>Ands the current expression with a new Double expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="compareOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> And(Expression<Func<TEntity, Double?>> property, CompareOp compareOp, Double value) {
         return Conjunction(BuildPropertyExpr(property, compareOp, value.ToString()));
      }

      /// <summary>Ands the current expression with a new DateTimeOffset expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="compareOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> And(Expression<Func<TEntity, DateTimeOffset>> property, CompareOp compareOp, DateTimeOffset value) {
         return Conjunction(BuildPropertyExpr(property, compareOp, Format(value)));
      }

      /// <summary>Ands the current expression with a new DateTimeOffset expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="compareOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> And(Expression<Func<TEntity, DateTimeOffset?>> property, CompareOp compareOp, DateTimeOffset value) {
         return Conjunction(BuildPropertyExpr(property, compareOp, Format(value)));
      }

      /// <summary>Ands the current expression with a new Guid expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="equalOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> And(Expression<Func<TEntity, Guid>> property, EqualOp equalOp, Guid value) {
         return Conjunction(BuildPropertyExpr(property, (CompareOp)equalOp, Format(value)));
      }

      /// <summary>Ands the current expression with a new Guid expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="equalOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> And(Expression<Func<TEntity, Guid?>> property, EqualOp equalOp, Guid value) {
         return Conjunction(BuildPropertyExpr(property, (CompareOp)equalOp, Format(value)));
      }

      /// <summary>Ands the current expression with a new byte array expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="equalOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> And(Expression<Func<TEntity, Byte[]>> property, EqualOp equalOp, Byte[] value) {
         return Conjunction(BuildPropertyExpr(property, (CompareOp)equalOp, Format(value)));
      }

      /// <summary>Ands the current expression with a new String expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="compareOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> And(Expression<Func<TEntity, String>> property, CompareOp compareOp, String value) {
         return Conjunction(BuildPropertyExpr(property, compareOp, "'" + value + "'"));
      }

      /// <summary>Ands the current expression with a new String expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="lowerValue">The lower value to compare the string property to.</param>
      /// <param name="rangeOp">The kind of comparison operation to perform.</param>
      /// <param name="upperValue">The upper value to compare the string property to.</param>
      /// <returns>The resulting expression.</returns>      
      public TableFilterBuilder<TEntity> And(Expression<Func<TEntity, String>> property, String lowerValue, RangeOp rangeOp = RangeOp.StartsWith, String upperValue = null) {
         upperValue = upperValue ?? NextStringKey(lowerValue);
         CompareOp lowerCompareOp = (CompareOp)((Int32)rangeOp >> 8);
         CompareOp upperCompareOp = (CompareOp)((Int32)rangeOp & 0xFF);
         return Conjunction(new TableFilterBuilder<TEntity>().And(property, lowerCompareOp, lowerValue).And(property, upperCompareOp, upperValue));
      }
      #endregion

      #region Or Members
      /// <summary>Ors a TableFilterBuilder expression with another.</summary>
      /// <param name="other">The other TableFilterBuidler expression.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> Or(TableFilterBuilder<TEntity> other) { return Conjunction(other); }

      /// <summary>Ors the current expression with a new Boolean expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="equalOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> Or(Expression<Func<TEntity, Boolean>> property, EqualOp equalOp, Boolean value) {
         return Conjunction(BuildPropertyExpr(property, (CompareOp)equalOp, Format(value)));
      }

      /// <summary>Ors the current expression with a new Boolean expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="equalOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> Or(Expression<Func<TEntity, Boolean?>> property, EqualOp equalOp, Boolean value) {
         return Conjunction(BuildPropertyExpr(property, (CompareOp)equalOp, Format(value)));
      }

      /// <summary>Ors the current expression with a new Int32 expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="compareOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> Or(Expression<Func<TEntity, Int32>> property, CompareOp compareOp, Int32 value) {
         return Conjunction(BuildPropertyExpr(property, compareOp, value.ToString()));
      }

      /// <summary>Ors the current expression with a new Int32 expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="compareOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> Or(Expression<Func<TEntity, Int32?>> property, CompareOp compareOp, Int32 value) {
         return Conjunction(BuildPropertyExpr(property, compareOp, value.ToString()));
      }

      /// <summary>Ors the current expression with a new Int64 expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="compareOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> Or(Expression<Func<TEntity, Int64>> property, CompareOp compareOp, Int64 value) {
         return Conjunction(BuildPropertyExpr(property, compareOp, value.ToString()));
      }

      /// <summary>Ors the current expression with a new Int64 expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="compareOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> Or(Expression<Func<TEntity, Int64?>> property, CompareOp compareOp, Int64 value) {
         return Conjunction(BuildPropertyExpr(property, compareOp, value.ToString()));
      }

      /// <summary>Ors the current expression with a new Double expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="compareOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> Or(Expression<Func<TEntity, Double>> property, CompareOp compareOp, Double value) {
         return Conjunction(BuildPropertyExpr(property, compareOp, value.ToString()));
      }

      /// <summary>Ors the current expression with a new Double expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="compareOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> Or(Expression<Func<TEntity, Double?>> property, CompareOp compareOp, Double value) {
         return Conjunction(BuildPropertyExpr(property, compareOp, value.ToString()));
      }

      /// <summary>Ors the current expression with a new DateTimeOffset expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="compareOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> Or(Expression<Func<TEntity, DateTimeOffset>> property, CompareOp compareOp, DateTimeOffset value) {
         return Conjunction(BuildPropertyExpr(property, compareOp, Format(value)));
      }

      /// <summary>Ors the current expression with a new DateTimeOffset expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="compareOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> Or(Expression<Func<TEntity, DateTimeOffset?>> property, CompareOp compareOp, DateTimeOffset value) {
         return Conjunction(BuildPropertyExpr(property, compareOp, Format(value)));
      }

      /// <summary>Ors the current expression with a new Guid expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="equalOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> Or(Expression<Func<TEntity, Guid>> property, EqualOp equalOp, Guid value) {
         return Conjunction(BuildPropertyExpr(property, (CompareOp)equalOp, Format(value)));
      }

      /// <summary>Ors the current expression with a new Guid expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="equalOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> Or(Expression<Func<TEntity, Guid?>> property, EqualOp equalOp, Guid value) {
         return Conjunction(BuildPropertyExpr(property, (CompareOp)equalOp, Format(value)));
      }

      /// <summary>Ors the current expression with a new byte array expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="equalOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> Or(Expression<Func<TEntity, Byte[]>> property, EqualOp equalOp, Byte[] value) {
         return Conjunction(BuildPropertyExpr(property, (CompareOp)equalOp, Format(value)));
      }

      /// <summary>Ors the current expression with a new String expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="compareOp">The kind of comparison operation to perform.</param>
      /// <param name="value">The value to compare the property to.</param>
      /// <returns>The resulting expression.</returns>
      public TableFilterBuilder<TEntity> Or(Expression<Func<TEntity, String>> property, CompareOp compareOp, String value) {
         return Conjunction(BuildPropertyExpr(property, compareOp, "'" + value + "'"));
      }

      /// <summary>Ors the current expression with a new String expression.</summary>
      /// <param name="property">A lambda expression identifying the type's property.</param>
      /// <param name="lowerValue">The lower value to compare the string property to.</param>
      /// <param name="rangeOp">The kind of comparison operation to perform.</param>
      /// <param name="upperValue">The upper value to compare the string property to.</param>
      /// <returns>The resulting expression.</returns>      
      public TableFilterBuilder<TEntity> Or(Expression<Func<TEntity, String>> property, String lowerValue, RangeOp rangeOp = RangeOp.StartsWith, String upperValue = null) {
         upperValue = upperValue ?? NextStringKey(lowerValue);
         CompareOp lowerCompareOp = (CompareOp)((Int32)rangeOp >> 8);
         CompareOp upperCompareOp = (CompareOp)((Int32)rangeOp & 0xFF);
         return Conjunction(new TableFilterBuilder<TEntity>().Or(property, lowerCompareOp, lowerValue).And(property, upperCompareOp, upperValue));
      }
      #endregion
   }
}
#endregion

#region TableQueryChunk
namespace Wintellect.Azure.Storage.Table {
   /// <summary>A class to help you query over a table's entities.</summary>
   public sealed class TableQueryChunk {
      private static readonly TableQuery s_emptyTableQuery = new TableQuery();
      private readonly CloudTable m_table;
      private readonly TableQuery m_query;
      private TableQuerySegment<DynamicTableEntity> m_segment = null;
      private IEnumerator<DynamicTableEntity> m_segmentEnumerator = null;
      private Int32 m_segmentRemainder = 0;
      /// <summary>Constructs a TableChunk over an AzureTable using the specified query.</summary>
      /// <param name="table">The table to query.</param>
      /// <param name="query">The query to apply to retrieve a table's entities.</param>
      public TableQueryChunk(AzureTable table, TableQuery query = null) { m_table = table; m_query = query ?? s_emptyTableQuery; }
      /// <summary>Returns true if you should call TaskAsync to retrieve some of the table's entities.</summary>
      public Boolean HasMore { get { return m_segmentRemainder > 0 || SegmentHasMore; } }
      private Boolean SegmentHasMore { get { return (m_segment == null) ? true : (SegmentContinuationToken != null); } }
      private TableContinuationToken SegmentContinuationToken {
         get { return (m_segment == null) ? null : m_segment.ContinuationToken; }
      }

      private IEnumerable<T> ToEnumerable<T>(IEnumerator<T> enumerator) {
         while (enumerator.MoveNext()) yield return enumerator.Current;
      }

      /// <summary>Efficiently retrieves some entities from a table.</summary>
      /// <param name="predicate">A function to filter out some of the retrieved entities.</param>
      /// <param name="requestOptions">A Microsoft.WindowsAzure.Storage.Table.TableRequestOptions object that specifies execution options, such as retry policy and timeout settings, for the operation.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A Task object representing some of the table's entities.</returns>
      public async Task<IEnumerable<DynamicTableEntity>> TakeAsync(Func<DynamicTableEntity, Boolean> predicate = null,
         TableRequestOptions requestOptions = null, OperationContext operationContext = null,
         CancellationToken cancellationToken = default(CancellationToken)) {
         predicate = predicate ?? (dte => true);
         if (m_segmentRemainder > 0) {
            // Segment has more, take the remainder of this segment
            var enumerator = m_segmentEnumerator;
            m_segmentRemainder = 0;
            m_segmentEnumerator = null;
            return ToEnumerable(enumerator).TakeWhile(predicate);
         }
         if (!SegmentHasMore) return Enumerable.Empty<DynamicTableEntity>();

         m_segment = await m_table.ExecuteQuerySegmentedAsync(m_query, SegmentContinuationToken,
            requestOptions, operationContext, cancellationToken).ConfigureAwait(false);
         IEnumerable<DynamicTableEntity> results = m_segment.Results.TakeWhile(predicate);
         if (m_query.SelectColumns != null && m_query.SelectColumns.Count > 0)
            results = results.Select(dte => dte.RemoveNullProperties());
         return results;
      }

      /// <summary>Retrieves chunkSize entities from a table (or less if at the end of the table).</summary>
      /// <param name="chunkSize">The number of entities to return (or less if at the end of the table.</param>
      /// <param name="predicate">A function to filter out some of the retrieved entities.</param>
      /// <param name="requestOptions">A Microsoft.WindowsAzure.Storage.Table.TableRequestOptions object that specifies execution options, such as retry policy and timeout settings, for the operation.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A Task object representing chunkSize entities (or less if at the end of the table).</returns>
      public async Task<IEnumerable<DynamicTableEntity>> TakeAsync(Int32 chunkSize, Func<DynamicTableEntity, Boolean> predicate = null,
         TableRequestOptions requestOptions = null, OperationContext operationContext = null,
         CancellationToken cancellationToken = default(CancellationToken)) {

         // Create chunk to return
         List<DynamicTableEntity> chunk = new List<DynamicTableEntity>(chunkSize);

         do {
            // Fill chunk from current segment
            while ((chunk.Count < chunk.Capacity) && (m_segmentEnumerator != null) && m_segmentEnumerator.MoveNext()) {
               DynamicTableEntity current = m_segmentEnumerator.Current;
               m_segmentRemainder--;
               if (predicate == null || predicate(current)) {
                  DynamicTableEntity dte = m_segmentEnumerator.Current;
                  if (m_query.SelectColumns != null && m_query.SelectColumns.Count > 0)
                     dte = dte.RemoveNullProperties();
                  chunk.Add(dte);
               }
            }
            if (chunk.Count == chunk.Capacity) break; // Return if chunk is full
            if (!SegmentHasMore) break; // Return if no more segments to read

            // Read next segment
            m_segment = await m_table.ExecuteQuerySegmentedAsync(m_query, SegmentContinuationToken,
               requestOptions, operationContext, cancellationToken).ConfigureAwait(false);
            m_segmentRemainder = m_segment.Count();
            m_segmentEnumerator = m_segment.Results.GetEnumerator();
         } while (true);
         return chunk;
      }
   }
}
#endregion

#region DynamicTableEntityExtensions
namespace Wintellect.Azure.Storage.Table {
   /// <summary>Defines DynamicTableEntity extension methods.</summary>
   public static class DynamicTableEntityExtensions {
      /// <summary>Removes any EntityProperties having a 'null' value from a DynamicTableEntity.</summary>
      /// <param name="dte">The DynamicTableEntity to examine.</param>
      /// <returns>The same DynamicTableEntity after the 'null' EntityProperties have been removed.</returns>
      public static DynamicTableEntity RemoveNullProperties(this DynamicTableEntity dte) {
         foreach (String key in dte.Properties.Keys.ToArray())
            if (dte.Properties[key].PropertyAsObject == null)
               dte.Properties.Remove(key);
         return dte;
      }

      #region Property Get & Set methods
      /// <summary>Gets a DynamicTableEntity's Boolean property.</summary>
      /// <param name="dte">The DynamicTableEntity defining the property.</param>
      /// <param name="defaultValue">The value to return if the property doesn't exist.</param>
      /// <param name="tablePropertyName">The name of the property.</param>
      /// <returns>The property's value (or default value if the property doesn't exist.)</returns>
      public static Boolean Get(this DynamicTableEntity dte, Boolean defaultValue, [CallerMemberName] String tablePropertyName = null) {
         EntityProperty value;
         return dte.Properties.TryGetValue(tablePropertyName, out value)
            ? (value.BooleanValue ?? defaultValue) : defaultValue;
      }
      /// <summary>Sets a DynamicTableEntity's Boolean property.</summary>
      /// <param name="dte">The DynamicTableEntity defining the property.</param>
      /// <param name="value">The value to set the property to.</param>
      /// <param name="tablePropertyName">The name of the property.</param>
      public static void Set(this DynamicTableEntity dte, Boolean value, [CallerMemberName] String tablePropertyName = null) {
         EntityProperty ep;
         if (dte.Properties.TryGetValue(tablePropertyName, out ep)) ep.BooleanValue = value;
         else dte.Properties.Add(tablePropertyName, new EntityProperty(value));
      }

      /// <summary>Gets a DynamicTableEntity's Int32 property.</summary>
      /// <param name="dte">The DynamicTableEntity defining the property.</param>
      /// <param name="defaultValue">The value to return if the property doesn't exist.</param>
      /// <param name="tablePropertyName">The name of the property.</param>
      /// <returns>The property's value (or default value if the property doesn't exist.)</returns>
      public static Int32 Get(this DynamicTableEntity dte, Int32 defaultValue, [CallerMemberName] String tablePropertyName = null) {
         EntityProperty value;
         return dte.Properties.TryGetValue(tablePropertyName, out value)
            ? (value.Int32Value ?? defaultValue) : defaultValue;
      }
      /// <summary>Sets a DynamicTableEntity's Int32 property.</summary>
      /// <param name="dte">The DynamicTableEntity defining the property.</param>
      /// <param name="value">The value to set the property to.</param>
      /// <param name="tablePropertyName">The name of the property.</param>
      public static void Set(this DynamicTableEntity dte, Int32 value, [CallerMemberName] String tablePropertyName = null) {
         EntityProperty ep;
         if (dte.Properties.TryGetValue(tablePropertyName, out ep)) ep.Int32Value = value;
         else dte.Properties.Add(tablePropertyName, new EntityProperty(value));
      }

      /// <summary>Gets a DynamicTableEntity's Int64 property.</summary>
      /// <param name="dte">The DynamicTableEntity defining the property.</param>
      /// <param name="defaultValue">The value to return if the property doesn't exist.</param>
      /// <param name="tablePropertyName">The name of the property.</param>
      /// <returns>The property's value (or default value if the property doesn't exist.)</returns>
      public static Int64 Get(this DynamicTableEntity dte, Int64 defaultValue, [CallerMemberName] String tablePropertyName = null) {
         EntityProperty value;
         return dte.Properties.TryGetValue(tablePropertyName, out value)
            ? (value.Int64Value ?? defaultValue) : defaultValue;
      }
      /// <summary>Sets a DynamicTableEntity's Int64 property.</summary>
      /// <param name="dte">The DynamicTableEntity defining the property.</param>
      /// <param name="value">The value to set the property to.</param>
      /// <param name="tablePropertyName">The name of the property.</param>
      public static void Set(this DynamicTableEntity dte, Int64 value, [CallerMemberName] String tablePropertyName = null) {
         EntityProperty ep;
         if (dte.Properties.TryGetValue(tablePropertyName, out ep)) ep.Int64Value = value;
         else dte.Properties.Add(tablePropertyName, new EntityProperty(value));
      }

      /// <summary>Gets a DynamicTableEntity's Double property.</summary>
      /// <param name="dte">The DynamicTableEntity defining the property.</param>
      /// <param name="defaultValue">The value to return if the property doesn't exist.</param>
      /// <param name="tablePropertyName">The name of the property.</param>
      /// <returns>The property's value (or default value if the property doesn't exist.)</returns>
      public static Double Get(this DynamicTableEntity dte, Double defaultValue, [CallerMemberName] String tablePropertyName = null) {
         EntityProperty value;
         return dte.Properties.TryGetValue(tablePropertyName, out value)
            ? (value.DoubleValue ?? defaultValue) : defaultValue;
      }
      /// <summary>Sets a DynamicTableEntity's Double property.</summary>
      /// <param name="dte">The DynamicTableEntity defining the property.</param>
      /// <param name="value">The value to set the property to.</param>
      /// <param name="tablePropertyName">The name of the property.</param>
      public static void Set(this DynamicTableEntity dte, Double value, [CallerMemberName] String tablePropertyName = null) {
         EntityProperty ep;
         if (dte.Properties.TryGetValue(tablePropertyName, out ep)) ep.DoubleValue = value;
         else dte.Properties.Add(tablePropertyName, new EntityProperty(value));
      }

      /// <summary>Gets a DynamicTableEntity's Guid property.</summary>
      /// <param name="dte">The DynamicTableEntity defining the property.</param>
      /// <param name="defaultValue">The value to return if the property doesn't exist.</param>
      /// <param name="tablePropertyName">The name of the property.</param>
      /// <returns>The property's value (or default value if the property doesn't exist.)</returns>
      public static Guid Get(this DynamicTableEntity dte, Guid defaultValue, [CallerMemberName] String tablePropertyName = null) {
         EntityProperty value;
         return dte.Properties.TryGetValue(tablePropertyName, out value)
            ? (value.GuidValue ?? defaultValue) : defaultValue;
      }
      /// <summary>Sets a DynamicTableEntity's Guid property.</summary>
      /// <param name="dte">The DynamicTableEntity defining the property.</param>
      /// <param name="value">The value to set the property to.</param>
      /// <param name="tablePropertyName">The name of the property.</param>
      public static void Set(this DynamicTableEntity dte, Guid value, [CallerMemberName] String tablePropertyName = null) {
         EntityProperty ep;
         if (dte.Properties.TryGetValue(tablePropertyName, out ep)) ep.GuidValue = value;
         else dte.Properties.Add(tablePropertyName, new EntityProperty(value));
      }

      /// <summary>Gets a DynamicTableEntity's DateTimeOffset property.</summary>
      /// <param name="dte">The DynamicTableEntity defining the property.</param>
      /// <param name="defaultValue">The value to return if the property doesn't exist.</param>
      /// <param name="tablePropertyName">The name of the property.</param>
      /// <returns>The property's value (or default value if the property doesn't exist.)</returns>
      public static DateTimeOffset Get(this DynamicTableEntity dte, DateTimeOffset defaultValue, [CallerMemberName] String tablePropertyName = null) {
         EntityProperty value;
         return dte.Properties.TryGetValue(tablePropertyName, out value)
            ? (value.DateTimeOffsetValue ?? defaultValue) : defaultValue;
      }
      /// <summary>Sets a DynamicTableEntity's DateTimeOffset property.</summary>
      /// <param name="dte">The DynamicTableEntity defining the property.</param>
      /// <param name="value">The value to set the property to.</param>
      /// <param name="tablePropertyName">The name of the property.</param>
      public static void Set(this DynamicTableEntity dte, DateTimeOffset value, [CallerMemberName] String tablePropertyName = null) {
         EntityProperty ep;
         if (dte.Properties.TryGetValue(tablePropertyName, out ep)) ep.DateTimeOffsetValue = value;
         else dte.Properties.Add(tablePropertyName, new EntityProperty(value));
      }

      /// <summary>Gets a DynamicTableEntity's String property.</summary>
      /// <param name="dte">The DynamicTableEntity defining the property.</param>
      /// <param name="defaultValue">The value to return if the property doesn't exist.</param>
      /// <param name="tablePropertyName">The name of the property.</param>
      /// <returns>The property's value (or default value if the property doesn't exist.)</returns>
      public static String Get(this DynamicTableEntity dte, String defaultValue, [CallerMemberName] String tablePropertyName = null) {
         if (defaultValue == null) throw new ArgumentNullException("defaultValue");
         EntityProperty value;
         return dte.Properties.TryGetValue(tablePropertyName, out value)
            ? (value.StringValue ?? defaultValue) : defaultValue;
      }
      /// <summary>Sets a DynamicTableEntity's String property.</summary>
      /// <param name="dte">The DynamicTableEntity defining the property.</param>
      /// <param name="value">The value to set the property to.</param>
      /// <param name="tablePropertyName">The name of the property.</param>
      public static void Set(this DynamicTableEntity dte, String value, [CallerMemberName] String tablePropertyName = null) {
         if (value == null) throw new ArgumentNullException("value");
         EntityProperty ep;
         if (dte.Properties.TryGetValue(tablePropertyName, out ep)) ep.StringValue = value;
         else dte.Properties.Add(tablePropertyName, new EntityProperty(value));
      }

      /// <summary>Gets a DynamicTableEntity's byte array property.</summary>
      /// <param name="dte">The DynamicTableEntity defining the property.</param>
      /// <param name="defaultValue">The value to return if the property doesn't exist.</param>
      /// <param name="numProperties">The number of properties to split the byte array over.</param>
      /// <param name="tablePropertyName">The name of the property.</param>
      /// <returns>The property's value (or default value if the property doesn't exist.)</returns>
      public static Byte[] Get(this DynamicTableEntity dte, Byte[] defaultValue, Int32 numProperties = 1, [CallerMemberName] String tablePropertyName = null) {
         if (defaultValue == null) throw new ArgumentNullException("defaultValue");
         if (numProperties < 1 || numProperties > 16) // 1MB / 64KB = 16
            throw new ArgumentException("maxProperties must be between 1 and 16", "maxProperties");
         if (numProperties == 1) {
            // High performance path for common scenario
            EntityProperty value;
            return dte.Properties.TryGetValue(tablePropertyName, out value)
               ? (value.BinaryValue ?? defaultValue) : defaultValue;
         }

         List<Byte> data = new List<Byte>();
         for (Int32 propertyNum = 0; propertyNum < numProperties; propertyNum++) {
            EntityProperty ep;
            var propName = tablePropertyName;
            if (propertyNum > 0) propName += propertyNum.ToString("D2");
            if (dte.Properties.TryGetValue(propName, out ep) && (ep.BinaryValue != null))
               data.AddRange(ep.BinaryValue);
         }
         return data.ToArray();
      }

      private static readonly Byte[] s_emptyByteArray = new Byte[0];
      /// <summary>Sets a DynamicTableEntity's byte array property.</summary>
      /// <param name="dte">The DynamicTableEntity defining the property.</param>
      /// <param name="value">The value to set the property to.</param>
      /// <param name="numProperties">The number of properties to join the byte array over.</param>
      /// <param name="tablePropertyName">The name of the property.</param>
      public static void Set(this DynamicTableEntity dte, Byte[] value, Int32 numProperties = 1, [CallerMemberName] String tablePropertyName = null) {
         if (value == null) throw new ArgumentNullException("value");
         if (numProperties < 1 || numProperties > 16) // 1MB / 64KB = 16
            throw new ArgumentException("maxProperties must be between 1 and 16", "maxProperties");

         if (numProperties == 1) {
            // High performance path for common scenario (not array allocation & copy)
            EntityProperty ep;
            if (dte.Properties.TryGetValue(tablePropertyName, out ep)) ep.BinaryValue = value;
            else dte.Properties.Add(tablePropertyName, new EntityProperty(value));
            return;
         }

         Byte[] array = null;
         for (Int32 propertyNum = 0; propertyNum < numProperties; propertyNum++) {
            var chunkSize = Math.Min(64 * 1024, value.Length - (propertyNum * 64 * 1024));
            if (chunkSize > 0) {
               if (array == null) array = new Byte[chunkSize];
               else if (array.Length > chunkSize) array = new Byte[chunkSize];
               Array.Copy(value, 64 * 1024 * propertyNum, array, 0, array.Length);
            } else array = s_emptyByteArray;
            var propName = tablePropertyName;
            if (propertyNum > 0) propName += propertyNum.ToString("D2");
            EntityProperty ep;
            if (dte.Properties.TryGetValue(propName, out ep)) ep.BinaryValue = array;
            else dte.Properties.Add(propName, new EntityProperty(array));
         }
      }
      #endregion
   }
}
#endregion

#region EntityBase
namespace Wintellect.Azure.Storage.Table {
   /// <summary>Common base class for a class that wraps a DynamicTableEntity.</summary>
   public abstract class EntityBase : ITableEntity {
      void ITableEntity.ReadEntity(IDictionary<String, EntityProperty> properties, OperationContext operationContext) {
         Entity.ReadEntity(properties, operationContext);
      }
      IDictionary<String, EntityProperty> ITableEntity.WriteEntity(OperationContext operationContext) {
         return Entity.WriteEntity(operationContext);
      }
      /// <summary>Retrieves an entity from the table and maps it to a DynamicTableEntity.</summary>
      /// <param name="at">The table to retrieve the entity from.</param>
      /// <param name="partitionKey">The entity's partition key value.</param>
      /// <param name="rowKey">The entity's row key value.</param>
      /// <returns>A Task wrapping the entity's DynamicTableEntity.</returns>
      public static async Task<DynamicTableEntity> FindAsync(AzureTable at, String partitionKey, String rowKey) {
         TableResult tr = await at.ExecuteAsync(TableOperation.Retrieve(partitionKey.HtmlEncode(), rowKey.HtmlEncode())).ConfigureAwait(false);
         if ((HttpStatusCode)tr.HttpStatusCode == HttpStatusCode.NotFound)
            throw new EntityNotFoundException(at, partitionKey, rowKey);
         return (DynamicTableEntity)tr.Result;
      }

      private readonly DynamicTableEntity Entity;
      /// <summary>Constructs a class wrapper around a DynamicTableEntity.</summary>
      /// <param name="dte">The DynamicTableEntity to wrap.</param>
      protected EntityBase(DynamicTableEntity dte = null) {
         Entity = dte ?? new DynamicTableEntity();
      }

      #region TableBatchOperation control
      /// <summary>Adds an Insert operation for this entity to a TableBatchOperation collection.</summary>
      /// <param name="tbo">The TableBatchOperation to add this operation to.</param>
      /// <returns>The same TableBatchOperation object passed in.</returns>
      public TableBatchOperation Insert(TableBatchOperation tbo) { tbo.Insert(Entity); return tbo; }
      /// <summary>Adds an InsertOrMerge operation for this entity to a TableBatchOperation collection.</summary>
      /// <param name="tbo">The TableBatchOperation to add this operation to.</param>
      /// <returns>The same TableBatchOperation object passed in.</returns>
      public TableBatchOperation InsertOrMerge(TableBatchOperation tbo) { tbo.InsertOrMerge(Entity); return tbo; }
      /// <summary>Adds an InsertOrReplace operation for this entity to a TableBatchOperation collection.</summary>
      /// <param name="tbo">The TableBatchOperation to add this operation to.</param>
      /// <returns>The same TableBatchOperation object passed in.</returns>
      public TableBatchOperation InsertOrReplace(TableBatchOperation tbo) { tbo.InsertOrReplace(Entity); return tbo; }
      /// <summary>Adds a Merge operation for this entity to a TableBatchOperation collection.</summary>
      /// <param name="tbo">The TableBatchOperation to add this operation to.</param>
      /// <returns>The same TableBatchOperation object passed in.</returns>
      public TableBatchOperation Merge(TableBatchOperation tbo) { tbo.Merge(Entity); return tbo; }
      /// <summary>Adds a Replace operation for this entity to a TableBatchOperation collection.</summary>
      /// <param name="tbo">The TableBatchOperation to add this operation to.</param>
      /// <returns>The same TableBatchOperation object passed in.</returns>
      public TableBatchOperation Replace(TableBatchOperation tbo) { tbo.Replace(Entity); return tbo; }
      /// <summary>Adds a Delete operation for this entity to a TableBatchOperation collection.</summary>
      /// <param name="tbo">The TableBatchOperation to add this operation to.</param>
      /// <returns>The same TableBatchOperation object passed in.</returns>
      public TableBatchOperation Delete(TableBatchOperation tbo) { tbo.Delete(Entity); return tbo; }
      #endregion

      #region Common properties: ETag, PartitionKey, RowKey, Timestamp
      /// <summary>Gets or sets the entity's current ETag.</summary>
      /// <remarks>Set this value to '*' to blindly overwrite an entity as part of an update operation.</remarks>
      public String ETag {
         get { return Entity.ETag; }
         set { Entity.ETag = value; }
      }
      /// <summary>Gets or sets the entity's partition key.</summary>
      public String PartitionKey {
         get { return Entity.PartitionKey; }
         set {
            if (!AzureTable.IsValidKey(value)) throw new InvalidTableKeyException(value);
            Entity.PartitionKey = value;
         }
      }
      /// <summary>Gets or sets the entity's row key.</summary>
      public String RowKey {
         get { return Entity.RowKey; }
         set {
            if (!AzureTable.IsValidKey(value)) throw new InvalidTableKeyException(value);
            Entity.RowKey = value;
         }
      }
      /// <summary>Gets or sets the entity's timestamp.</summary>
      public DateTimeOffset Timestamp {
         get { return Entity.Timestamp; }
         set { Entity.Timestamp = value; }
      }
      #endregion

      #region Inherited entity property getters & setters
      /// <summary>Gets the property's value or defaultValue if this property doesn't exist.</summary>
      /// <param name="defaultValue">Value to return if property is not found on entity.</param>
      /// <param name="tablePropertyName">Name of property to return.</param>
      /// <returns>Property's value or defaultValue.</returns>
      protected Boolean Get(Boolean defaultValue, [CallerMemberName] String tablePropertyName = null) {
         return Entity.Get(defaultValue, tablePropertyName);
      }
      /// <summary>Sets the property's value.</summary>
      /// <param name="value">Value property should be set to.</param>
      /// <param name="tablePropertyName">Name of property to set.</param>
      protected void Set(Boolean value, [CallerMemberName] String tablePropertyName = null) { Entity.Set(value, tablePropertyName); }

      /// <summary>Gets the property's value or defaultValue if this property doesn't exist.</summary>
      /// <param name="defaultValue">Value to return if property is not found on entity.</param>
      /// <param name="tablePropertyName">Name of property to return.</param>
      /// <returns>Property's value or defaultValue.</returns>
      protected Int32 Get(Int32 defaultValue, [CallerMemberName] String tablePropertyName = null) {
         return Entity.Get(defaultValue, tablePropertyName);
      }
      /// <summary>Sets the property's value.</summary>
      /// <param name="value">Value property should be set to.</param>
      /// <param name="tablePropertyName">Name of property to set.</param>
      protected void Set(Int32 value, [CallerMemberName] String tablePropertyName = null) { Entity.Set(value, tablePropertyName); }

      /// <summary>Gets the property's value or defaultValue if this property doesn't exist.</summary>
      /// <param name="defaultValue">Value to return if property is not found on entity.</param>
      /// <param name="tablePropertyName">Name of property to return.</param>
      /// <returns>Property's value or defaultValue.</returns>
      protected Int64 Get(Int64 defaultValue, [CallerMemberName] String tablePropertyName = null) {
         return Entity.Get(defaultValue, tablePropertyName);
      }
      /// <summary>Sets the property's value.</summary>
      /// <param name="value">Value property should be set to.</param>
      /// <param name="tablePropertyName">Name of property to set.</param>
      protected void Set(Int64 value, [CallerMemberName] String tablePropertyName = null) { Entity.Set(value, tablePropertyName); }

      /// <summary>Gets the property's value or defaultValue if this property doesn't exist.</summary>
      /// <param name="defaultValue">Value to return if property is not found on entity.</param>
      /// <param name="tablePropertyName">Name of property to return.</param>
      /// <returns>Property's value or defaultValue.</returns>
      protected Double Get(Double defaultValue, [CallerMemberName] String tablePropertyName = null) {
         return Entity.Get(defaultValue, tablePropertyName);
      }
      /// <summary>Sets the property's value.</summary>
      /// <param name="value">Value property should be set to.</param>
      /// <param name="tablePropertyName">Name of property to set.</param>
      protected void Set(Double value, [CallerMemberName] String tablePropertyName = null) { Entity.Set(value, tablePropertyName); }

      /// <summary>Gets the property's value or defaultValue if this property doesn't exist.</summary>
      /// <param name="defaultValue">Value to return if property is not found on entity.</param>
      /// <param name="tablePropertyName">Name of property to return.</param>
      /// <returns>Property's value or defaultValue.</returns>
      protected Guid Get(Guid defaultValue, [CallerMemberName] String tablePropertyName = null) {
         return Entity.Get(defaultValue, tablePropertyName);
      }
      /// <summary>Sets the property's value.</summary>
      /// <param name="value">Value property should be set to.</param>
      /// <param name="tablePropertyName">Name of property to set.</param>
      protected void Set(Guid value, [CallerMemberName] String tablePropertyName = null) { Entity.Set(value, tablePropertyName); }

      /// <summary>Gets the property's value or defaultValue if this property doesn't exist.</summary>
      /// <param name="defaultValue">Value to return if property is not found on entity.</param>
      /// <param name="tablePropertyName">Name of property to return.</param>
      /// <returns>Property's value or defaultValue.</returns>
      protected DateTimeOffset Get(DateTimeOffset defaultValue, [CallerMemberName] String tablePropertyName = null) {
         return Entity.Get(defaultValue, tablePropertyName);
      }
      /// <summary>Sets the property's value.</summary>
      /// <param name="value">Value property should be set to.</param>
      /// <param name="tablePropertyName">Name of property to set.</param>
      protected void Set(DateTimeOffset value, [CallerMemberName] String tablePropertyName = null) { Entity.Set(value, tablePropertyName); }

      /// <summary>Gets the property's value or defaultValue if this property doesn't exist.</summary>
      /// <param name="defaultValue">Value to return if property is not found on entity.</param>
      /// <param name="tablePropertyName">Name of property to return.</param>
      /// <returns>Property's value or defaultValue.</returns>
      protected String Get(String defaultValue, [CallerMemberName] String tablePropertyName = null) {
         return Entity.Get(defaultValue, tablePropertyName);
      }
      /// <summary>Sets the property's value.</summary>
      /// <param name="value">Value property should be set to.</param>
      /// <param name="tablePropertyName">Name of property to set.</param>
      protected void Set(String value, [CallerMemberName] String tablePropertyName = null) { Entity.Set(value, tablePropertyName); }

      /// <summary>Gets the property's value or defaultValue if this property doesn't exist.</summary>
      /// <param name="defaultValue">Value to return if property is not found on entity.</param>
      /// <param name="numProperties">The number of properties to split the byte array over.</param>
      /// <param name="tablePropertyName">Name of property to return.</param>
      /// <returns>Property's value or defaultValue.</returns>
      protected Byte[] Get(Byte[] defaultValue, Int32 numProperties = 1, [CallerMemberName] String tablePropertyName = null) {
         return Entity.Get(defaultValue, numProperties, tablePropertyName);
      }

      /// <summary>Sets the property's value.</summary>
      /// <param name="value">Value property should be set to.</param>
      /// <param name="numProperties">The number of properties to join the byte array over.</param>
      /// <param name="tablePropertyName">Name of property to set.</param>
      protected void Set(Byte[] value, Int32 numProperties = 1, [CallerMemberName] String tablePropertyName = null) {
         Entity.Set(value, numProperties, tablePropertyName);
      }
      #endregion
   }

   /// <summary>Associates a table with an entity making it easy to manipulate the entity on a specific table.</summary>
   public class TableEntityBase : EntityBase {
      private readonly AzureTable m_azureTable;
      /// <summary>Constructs a class wrapper around a DynamicTableEntity and its table.</summary>
      /// <param name="at">The table that the entity is associated with.</param>
      /// <param name="dte">The DynamicTableEntity to wrap.</param>
      protected TableEntityBase(AzureTable at, DynamicTableEntity dte = null)
         : base(dte) {
         m_azureTable = at;
      }

      #region CloudTable control
      /// <summary>Inserts this entity into its table.</summary>
      /// <param name="requestOptions">A Microsoft.WindowsAzure.Storage.Table.TableRequestOptions object that specifies execution options, such as retry policy and timeout settings, for the operation.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>The result of the table operation.</returns>
      public Task<TableResult> InsertAsync(TableRequestOptions requestOptions = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return m_azureTable.ExecuteAsync(TableOperation.Insert(this), requestOptions, operationContext, cancellationToken);
      }
      /// <summary>Inserts or merges this entity into its table.</summary>
      /// <param name="requestOptions">A Microsoft.WindowsAzure.Storage.Table.TableRequestOptions object that specifies execution options, such as retry policy and timeout settings, for the operation.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>The result of the table operation.</returns>
      public Task<TableResult> InsertOrMergeAsync(TableRequestOptions requestOptions = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return m_azureTable.ExecuteAsync(TableOperation.InsertOrMerge(this), requestOptions, operationContext, cancellationToken);
      }
      /// <summary>Inserts or replaces this entity into its table.</summary>
      /// <param name="requestOptions">A Microsoft.WindowsAzure.Storage.Table.TableRequestOptions object that specifies execution options, such as retry policy and timeout settings, for the operation.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>The result of the table operation.</returns>
      public Task<TableResult> InsertOrReplaceAsync(TableRequestOptions requestOptions = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return m_azureTable.ExecuteAsync(TableOperation.InsertOrReplace(this), requestOptions, operationContext, cancellationToken);
      }
      /// <summary>Merges this entity into its table.</summary>
      /// <param name="requestOptions">A Microsoft.WindowsAzure.Storage.Table.TableRequestOptions object that specifies execution options, such as retry policy and timeout settings, for the operation.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>The result of the table operation.</returns>
      public Task<TableResult> MergeAsync(TableRequestOptions requestOptions = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return m_azureTable.ExecuteAsync(TableOperation.Merge(this), requestOptions, operationContext, cancellationToken);
      }
      /// <summary>Replaces this entity on into its table.</summary>
      /// <param name="requestOptions">A Microsoft.WindowsAzure.Storage.Table.TableRequestOptions object that specifies execution options, such as retry policy and timeout settings, for the operation.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>The result of the table operation.</returns>
      public Task<TableResult> ReplaceAsync(TableRequestOptions requestOptions = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return m_azureTable.ExecuteAsync(TableOperation.Replace(this), requestOptions, operationContext, cancellationToken);
      }
      /// <summary>Deletes this entity from its table.</summary>
      /// <param name="requestOptions">A Microsoft.WindowsAzure.Storage.Table.TableRequestOptions object that specifies execution options, such as retry policy and timeout settings, for the operation.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>The result of the table operation.</returns>
      public Task<TableResult> DeleteAsync(TableRequestOptions requestOptions = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return m_azureTable.ExecuteAsync(TableOperation.Delete(this), requestOptions, operationContext, cancellationToken);
      }
      #endregion
   }

   /// <summary>Used to indicate when an entity cannot be found in a table.</summary>
   public sealed class EntityNotFoundException : Exception {

      internal EntityNotFoundException(AzureTable table, String partitionKey, String rowKey) : base() { Table = table; PartitionKey = partitionKey; RowKey = rowKey; }
      /// <summary>The table the entity was searched within.</summary>
      public AzureTable Table { get; private set; }
      /// <summary>The partition key of the entity that was not found.</summary>
      public String PartitionKey { get; private set; }
      /// <summary>The row key of the entity that was not found.</summary>
      public String RowKey { get; private set; }
   }

   /// <summary>Indicates that a string has 1 or more characters that are invalid for a table's partition or row key.</summary>
   public sealed class InvalidTableKeyException : Exception {
      internal InvalidTableKeyException(String key, String message = null, Exception innerException = null)
         : base(message, innerException) {
         Key = key;
      }
      /// <summary>The key containing invalid characters.</summary>
      public readonly String Key;
   }
}
#endregion

#region PropertyReplacer & PropertyChanger
namespace Wintellect.Azure.Storage.Table {
   /// <summary>Each instance indicates a property to keep or rename.</summary>
   public struct PropertyReplacer {
      internal String CurrentPropertyName, NewPropertyName;
      /// <summary>Indicates a property to keep and optionally what to rename the property to.</summary>
      /// <param name="currentPropertyName">The current name of the property.</param>
      /// <param name="newPropertyName">The new name of the property (or null to keep the name the same).</param>
      public PropertyReplacer(String currentPropertyName, String newPropertyName = null) {
         CurrentPropertyName = currentPropertyName;
         NewPropertyName = newPropertyName ?? currentPropertyName;
      }
      internal Boolean Changing { get { return CurrentPropertyName != NewPropertyName; } }
      /// <summary>Implicitly converts a property string name to a PropertyReplacer instance.</summary>
      /// <param name="currentPropertyName">The current name of the property.</param>
      /// <returns>The PropertyReplacer instance.</returns>
      public static implicit operator PropertyReplacer(String currentPropertyName) {
         return new PropertyReplacer(currentPropertyName);
      }
   }

   /// <summary>Use an instance of this class to scan a table to keep, rename, or remove properties form all entities.</summary>
   public sealed class PropertyChanger {
      private readonly AzureTable m_table;
      /// <summary>Constructs an instance of the class indicating which table to scan.</summary>
      /// <param name="table">The table to scan.</param>
      public PropertyChanger(AzureTable table) { m_table = table; }

      /// <summary>Scans the table's entities keeping and optionally renaming the specified properties.</summary>
      /// <param name="propertyNamesToKeepAndRename">The collection of properties to keep/rename; properties not mentioned in this collection are destroyed.</param>
      public async Task KeepOnlyAsync(params PropertyReplacer[] propertyNamesToKeepAndRename) {
         if (propertyNamesToKeepAndRename.Length == 0)
            throw new ArgumentException("propertyNamesToKeepAndRename must have some elements in it.");

         TableQuery tq = new TableQuery {
            SelectColumns = propertyNamesToKeepAndRename.Select(pr => pr.CurrentPropertyName).ToList()
         };
         for (var tqc = m_table.CreateQueryChunker(tq); tqc.HasMore; ) {
            var chunk = await tqc.TakeAsync(null, null, null, CancellationToken.None).ConfigureAwait(false);
            foreach (DynamicTableEntity dte in chunk) {
               foreach (PropertyReplacer pr in propertyNamesToKeepAndRename) {
                  if (!pr.Changing) continue;
                  if (!dte.Properties.ContainsKey(pr.CurrentPropertyName)) continue;
                  var ep = dte.Properties[pr.CurrentPropertyName];
                  dte.Properties.Remove(pr.CurrentPropertyName);
                  dte.Properties.Add(pr.NewPropertyName, ep);
               }
               await m_table.ExecuteAsync(TableOperation.Replace(dte)).ConfigureAwait(false);
            }
         }
      }
      /// <summary>Scans the table's entities removing any specified properties.</summary>      
      /// <param name="propertyNamesToRemove">The collection of properties to remove from each entity.</param>
      public async Task RemoveAsync(params String[] propertyNamesToRemove) {
         for (var tqc = m_table.CreateQueryChunker(); tqc.HasMore; ) {
            var chunk = await tqc.TakeAsync(null, null, null, CancellationToken.None).ConfigureAwait(false);
            foreach (DynamicTableEntity dte in chunk) {
               Boolean anyPropertiesRemoved = false;
               foreach (var name in propertyNamesToRemove) {
                  anyPropertiesRemoved = dte.Properties.Remove(name) || anyPropertiesRemoved;
               }
               if (anyPropertiesRemoved) await m_table.ExecuteAsync(TableOperation.Replace(dte)).ConfigureAwait(false);
            }
         }
      }
   }
}
#endregion

#if DataServiceContextSerialization
namespace Wintellect.Azure.Storage.Table {
   public static class DataServiceContextSerializer {
      public static T Deserialize<T>(this T context, IEnumerable<SerializedEntity> serializedEntities) where T : DataServiceContext {
         foreach (var ce in serializedEntities) {
            Object entity = CopyProperties(Activator.CreateInstance(BuildEntityType(context, ce)), ce.Properties);

            switch (ce.State) {
               case EntityStates.Added: context.AddObject(ce.TableName, entity); break;
               case EntityStates.Deleted: context.AttachTo(ce.TableName, entity); context.DeleteObject(entity); break;
               case EntityStates.Modified: context.AttachTo(ce.TableName, entity, ce.ETag); context.UpdateObject(entity); break;
               default: Debug.Fail("Unexpected state"); break;
            }
         }
         return context;
      }

#region Private Helper Members
      private static readonly ModuleBuilder s_moduleBuilder;
      static DataServiceContextSerializer() {
         AssemblyName assemblyName = new AssemblyName("Wintellect.WintellectAzure.CloudStorage.DynamicEntityTypes");
         var assemblerBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.RunAndCollect);
         s_moduleBuilder = assemblerBuilder.DefineDynamicModule(assemblyName.Name);
      }

      private static Type BuildEntityType(DataServiceContext dsc, SerializedEntity entity) {
         // The type name is built from the entity property names (in alphabetical order) and datatypes
         // We reaplce "[]" with "Array" because the CLR changes "[]" to "\[\]" which causes ModuleBuilder.GetType(...) to fail below
         String typeName = String.Join("_", from p in entity.Properties orderby p.Name select p.Name + "_" + p.Type).Replace("[]", "Array");

         // if the type exists, return it
         Type type = s_moduleBuilder.GetType(typeName);
         if (type != null) return type;

         // Define type with properties matching deserialized state
         TypeBuilder tb = s_moduleBuilder.DefineType(typeName, TypeAttributes.Public, typeof(TableEntity));

         // Define default ctor that invokes base class ctor
         ConstructorBuilder ctor = tb.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
         ctor.GetILGenerator().EmitX(OpCodes.Ldarg_0).EmitX(OpCodes.Call, tb.BaseType.GetConstructor(Type.EmptyTypes)).Emit(OpCodes.Ret);

         // For each property, define private field & public get/set accessor methods
         foreach (var p in entity.Properties) {
            // Don't define these properties since type is derived from TableEntity
            if (p.Name == "PartitionKey" || p.Name == "RowKey" || p.Name == "Timestamp") continue;

            Type propertyType = Type.GetType("System." + p.Type); // Because Capture removes "System"
            FieldBuilder fieldBuilder = tb.DefineField(p.Name, propertyType, FieldAttributes.Private);

            PropertyBuilder propertyBuilder = tb.DefineProperty(p.Name, PropertyAttributes.HasDefault, propertyType, null);
            MethodAttributes propMethodAttrs = MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig;

            MethodBuilder getAccessor = tb.DefineMethod("get_" + p.Name, propMethodAttrs, propertyType, null);
            getAccessor.GetILGenerator().EmitX(OpCodes.Ldarg_0).EmitX(OpCodes.Ldfld, fieldBuilder).Emit(OpCodes.Ret);

            MethodBuilder setAccessor = tb.DefineMethod("set_" + p.Name, propMethodAttrs, null, new Type[] { propertyType });
            setAccessor.GetILGenerator().EmitX(OpCodes.Ldarg_0).EmitX(OpCodes.Ldarg_1).EmitX(OpCodes.Stfld, fieldBuilder).Emit(OpCodes.Ret);

            propertyBuilder.SetGetMethod(getAccessor);
            propertyBuilder.SetSetMethod(setAccessor);
         }
         return tb.CreateType();
      }

      private static Object CopyProperties(Object destination, IEnumerable<SerializedEntity.EntityProperty> properties) {
         foreach (var p in properties) {
            var pi = destination.GetType().GetProperty(p.Name);   // Find the property
            Object value;                                         // Holds the value to set in the property

            // Convert the String to the correct property type
            if (pi.PropertyType == typeof(Byte[])) value = Convert.FromBase64String(p.Value);
            else if (pi.PropertyType == typeof(Guid)) value = Guid.Parse(p.Value);
            else value = Convert.ChangeType(p.Value, Type.GetType("System." + p.Type));
            pi.SetValue(destination, value, null); // Set the property's value
         }
         return destination;
      }

      private static ILGenerator EmitX(this ILGenerator ilGenerator, OpCode opcode) {
         ilGenerator.Emit(opcode); return ilGenerator;
      }
      private static ILGenerator EmitX(this ILGenerator ilGenerator, OpCode opcode, ConstructorInfo con) {
         ilGenerator.Emit(opcode, con); return ilGenerator;
      }

      private static ILGenerator EmitX(this ILGenerator ilGenerator, OpCode opcode, FieldInfo field) {
         ilGenerator.Emit(opcode, field); return ilGenerator;
      }
#endregion
   }
}
#endif




#if false
public sealed class ScalableCounter : IDisposable {
   private readonly CloudTable m_cloudTable;
   private readonly String m_counterName; // PartitionKey
   private readonly String m_roleInstanceId = RoleEnvironment.CurrentRoleInstance.Id;  // RowKey
   private readonly Timer m_timer = null;
   private CounterEntity m_counterEntity;
   private Int64 m_count = 0;

   private sealed class CounterEntity : TableEntity {
      public CounterEntity() { /* for deserialization */ }
      public CounterEntity(String counterName, String roleInstanceId)
         : base(counterName, roleInstanceId) { Counter = 0; }
      public Int64 Counter { get; set; }
   }

   public ScalableCounter(CloudTable cloudTable, String counterName, TimeSpan updateFrequency) {
      m_cloudTable = cloudTable;
      m_counterName = counterName;
      m_timer = new Timer(UpdateTable, null, updateFrequency, updateFrequency);
   }

   public void Dispose() {
      m_timer.Dispose();
      UpdateTable(null);   // Force one last update
   }

   public async Task Initialize() {
      await m_cloudTable.CreateIfNotExistsAsync();
      try {
         // NOTE: If a role instance recycles, its Id remains the same.
         var tr = await m_cloudTable.ExecuteAsync(TableOperation.Retrieve<CounterEntity>(m_counterName, m_roleInstanceId));
         m_count = ((CounterEntity)tr.Result).Counter;
      }
      // If table doesn't exist, create it.
      // If table exists but entity doesn't, insert it
      catch (Exception ex) {
         var m = ex.Message;
         m_counterEntity = new CounterEntity(m_counterName, m_roleInstanceId);
         await m_cloudTable.ExecuteAsync(TableOperation.Insert(m_counterEntity));
      }
   }

   public async Task<Int64> GetCurrentCount() {
      var query = new TableQuery<CounterEntity> { FilterString = "PartitionKey eq '" + m_counterName + "'" };
      Int64 sum = 0;
      for (TableQuerySegment<CounterEntity> tqs = null; tqs.HasMore(); ) {
         tqs = await m_cloudTable.ExecuteQuerySegmentedAsync(query, tqs.SafeContinuationToken());
         sum += tqs.Results.Sum(ce => ce.Counter);
      }
      return sum;
   }

   public void Add(Int32 value) { Interlocked.Add(ref m_count, value); }

   private async void UpdateTable(Object state) {
      // A timer executes this method periodically
      if (m_counterEntity.Counter != m_count) {
         m_counterEntity.Counter = m_count;
         await m_cloudTable.ExecuteAsync(TableOperation.Merge(m_counterEntity));
      }
   }
}
#endif