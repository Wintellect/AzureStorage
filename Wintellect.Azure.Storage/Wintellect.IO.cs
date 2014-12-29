/******************************************************************************
Module:  Wintellect.IO.cs
Notices: Copyright (c) by Jeffrey Richter & Wintellect
******************************************************************************/

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Wintellect.IO {
   // http://www.goodreads.com/author_blog_posts/2312502-asynchronous-batch-logging-without-blocking-threads
   using Batch = System.Collections.Generic.List<Byte[]>;

   /// <summary>A class that batches log information together persisting it when a batch (number of bytes) has been reached or when a period of time has expired.</summary>
   public sealed class BatchLogger {
      private static readonly TimeSpan Infinite = TimeSpan.FromMilliseconds(Timeout.Infinite);
      private SpinLock m_lock = new SpinLock(false);  // Can't be read-only since this is a struct
      private readonly TimeSpan m_maxInterval;
      private readonly Int32 m_maxBatchBytes;
      private readonly Func<Batch, Task> m_transferBatch;
      private Int32 m_outstandingBatches = 0;
      private Batch m_batch = null;
      private Int32 m_batchBytes = 0;
      private Timer m_timer = null;
      private Boolean m_minimalLossEnabled = false;

      /// <summary>Constructs a batch logger.</summary>
      /// <param name="maxBatchBytes">Indicates how many bytes need to accumulate in the batch in memory before persisting the batch.</param>
      /// <param name="maxInterval">The maximum size (in milliseconds) that bytes should sit in memory before being persisted.</param>
      /// <param name="transferBatch">The method to call to persist the batch.</param>
      public BatchLogger(Int32 maxBatchBytes, TimeSpan maxInterval, Func<Batch, Task> transferBatch) {
         m_maxBatchBytes = maxBatchBytes;
         m_maxInterval = maxInterval;
         m_transferBatch = transferBatch;
         CreateNewBatch();
      }

      // Returns reference to old batch so it can be flushed to the log
      private Batch CreateNewBatch() {
         // NOTE: This method MUST be called under the SpinLock unless called by the ctor
         if (m_batch != null && m_batchBytes == 0) return null;  // Batch exists & is empty, nothing to do
         Batch oldbatch = m_batch;
         m_batch = new Batch(m_maxBatchBytes);
         m_batchBytes = 0;
         if (m_timer != null) m_timer.Dispose();
         m_timer = new Timer(TimeExpired, m_batch, m_maxInterval, Infinite);
         return oldbatch;
      }

      private void TimeExpired(Object timersBatch) {
         Boolean taken = false;
         m_lock.Enter(ref taken);
         Batch oldBatch = null;
         if (timersBatch != m_batch) {
            // This timer is not for the current batch; do nothing
            // Solves race condition where time fires AS batch is changing
         } else {
            if (m_batchBytes == 0) {
               // Batch empty: reset timer & use the same batch
               m_timer.Change(m_maxInterval, Infinite);
            } else {
               // Batch not empty: (force flush &) create a new batch
               oldBatch = CreateNewBatch();
            }
         }
         m_lock.Exit();
         // If there was an old batch, transfer it
         TransferBatchAsync(this, oldBatch);
      }

      /// <summary>
      /// Call this method to flush the current batch and to disable batching. Future
      /// calls to Append will immediately call the transferBatch callback. You can call this 
      /// method when a VM wants to shut down to reduce the chance of losing log data.
      /// </summary>
      public void EnableMinimalLoss() {
         // Flush any existing batch
         Boolean taken = false;
         m_lock.Enter(ref taken);
         m_minimalLossEnabled = true;  // Future log entries are not batched
         Batch oldBatch = CreateNewBatch();
         m_lock.Exit();
         // If batch swapped, transfer it
         TransferBatchAsync(this, oldBatch);
      }

      /// <summary>Appends a byte array to the current batch.</summary>
      /// <param name="data">The byte array containing the data to append.</param>
      public void Append(Byte[] data) {
         if (data.Length > m_maxBatchBytes)
            throw new ArgumentOutOfRangeException("data", "A single data object cannot be larger than " + m_maxBatchBytes.ToString() + " bytes.");
         if (m_minimalLossEnabled) { TransferBatchAsync(this, new Batch { data }); return; }

         Boolean taken = false;
         m_lock.Enter(ref taken);
         Batch oldBatch = null;
         if (m_batchBytes + data.Length > m_maxBatchBytes) {
            // Data won't fit in current batch, create a new batch so we can transfer the old batch
            oldBatch = CreateNewBatch();
         }
         // Add new data to current (or new) batch
         m_batch.Add(data);
         m_batchBytes += data.Length;
         m_lock.Exit();
         // If batch swapped, transfer it
         TransferBatchAsync(this, oldBatch);
      }

      // TransferBatch is static to make it clear that this method is
      // NOT attached to the object in any way; this is an independent operation
      // that does not rely on any object state or a lock
      private static Task TransferBatchAsync(BatchLogger batchLogger, Batch batch) {
         // NOTE: this should be called while NOT under a lock
         if (batch == null) return Task.FromResult(true); // No batch to transfer, return

         // Start transfer of batch (asynchronously) to the persistent store...
         Interlocked.Increment(ref batchLogger.m_outstandingBatches);
         return batchLogger.m_transferBatch(batch)
            .ContinueWith(_ => Interlocked.Decrement(ref batchLogger.m_outstandingBatches));
      }
   }
}

namespace Wintellect.IO {
   /// <summary>A stream that efficiently stores a collection of byte arrays allowing you to read from them as a stream.</summary>
   public sealed class GatherStream : Stream {
      private readonly IEnumerable<Byte[]> m_enumerable;
      private readonly Int64 m_length;
      private IEnumerator<Byte[]> m_buffers;
      private Byte[] m_currentBuffer = null;
      private Int32 m_bufferOffset = 0;
      private Int64 m_position = 0;

      /// <summary>Constructs a GatherStream initializing it with a collection of byte arrays.</summary>
      /// <param name="buffers">The collection of bytes arrays to be read from as a stream.</param>
      public GatherStream(IEnumerable<Byte[]> buffers) {
         m_length = buffers.Sum(b => b.Length);
         m_enumerable = buffers;
         Seek(0, SeekOrigin.Begin);
      }
      /// <summary>Always returns true.</summary>
      public override Boolean CanSeek { get { return true; } }
      /// <summary>Seeks to a byte offset within the logical stream.</summary>
      /// <param name="offset">The offset of the byte within the logical stream (must be 0).</param>
      /// <param name="origin">The origin (must be SeekOrigin.Begin).</param>
      /// <returns>Always returns 0.</returns>
      public override Int64 Seek(Int64 offset, SeekOrigin origin) {
         if (offset != 0 || origin != SeekOrigin.Begin) throw new NotImplementedException();
         m_buffers = m_enumerable.GetEnumerator();
         m_currentBuffer = m_buffers.MoveNext() ? m_buffers.Current : null;
         m_bufferOffset = 0;
         m_position = 0;
         return 0;
      }
      /// <summary>The number of bytes in the logical stream.</summary>
      public override Int64 Length { get { return m_length; } }
      /// <summary>Always returns true.</summary>
      public override Boolean CanRead { get { return true; } }
      /// <summary>Copies bytes from the logical stream into a byte array.</summary>
      /// <param name="buffer">The buffer where the stream's bytes should be placed.</param>
      /// <param name="offset">The offset the buffer to place the stream's next byte.</param>
      /// <param name="count">The number of bytes from the stream to copy into the buffer.</param>
      /// <returns>The number of bytes copied into the buffer.</returns>
      public override Int32 Read(Byte[] buffer, Int32 offset, Int32 count) {
         Int32 bytesRead = 0;
         // Loop while we still have buffers and we need to read more bytes
         while (m_currentBuffer != null && bytesRead < count) {
            // Copy bytes from internal buffer to output buffer
            Int32 bytesToTransferThisTime = Math.Min(count - bytesRead, m_currentBuffer.Length - m_bufferOffset);
            Buffer.BlockCopy(m_currentBuffer, m_bufferOffset, buffer, offset, bytesToTransferThisTime);
            bytesRead += bytesToTransferThisTime;
            offset += bytesToTransferThisTime;
            m_bufferOffset += bytesToTransferThisTime;
            if (m_bufferOffset == m_currentBuffer.Length) {
               // Read all of this Byte[], move to next Byte[]
               m_bufferOffset = 0;
               m_currentBuffer = m_buffers.MoveNext() ? m_buffers.Current : null;
            }
         }
         m_position += bytesRead;
         return bytesRead;
      }
      #region Output members
      /// <summary>You can set the Position to 0 (only). You cannot get the position.</summary>
      public override Int64 Position {
         get { return m_position; }
         set { if (value != 0) throw new ArgumentOutOfRangeException("value must be 0"); Seek(0, SeekOrigin.Begin); }
      }
      /// <summary>Always returns false.</summary>
      public override Boolean CanWrite { get { return false; } }
      /// <summary>Throws a NotImplementedException.</summary>
      /// <param name="value">This argument is ignored.</param>
      public override void SetLength(Int64 value) { throw new NotImplementedException(); }
      /// <summary>Throws a NotImplementedException.</summary>
      /// <param name="buffer">This argument is ignored.</param>
      /// <param name="offset">This argument is ignored.</param>
      /// <param name="count">This argument is ignored.</param>
      public override void Write(Byte[] buffer, Int32 offset, Int32 count) { throw new NotImplementedException(); }
      /// <summary>Throws a NotImplementedException.</summary>
      public override void Flush() { throw new NotImplementedException(); }
      #endregion
   }
}
