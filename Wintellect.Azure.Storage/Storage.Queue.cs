/******************************************************************************
Module:  Storage.Queue.cs
Notices: Copyright (c) by Jeffrey Richter & Wintellect
******************************************************************************/

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Queue.Protocol;

namespace Wintellect.Azure.Storage.Queue {
   /// <summary>Defines extension methods useful when working with Azure storage queues.</summary>
   public static class QueueExtensions {
      /// <summary>Returns True for a null result segment or if the result segment has more.</summary>
      /// <param name="qrs">A QueueResultSegment (can be null).</param>
      /// <returns>True if qrs is null or if its continuation token is not null.</returns>
      public static Boolean HasMore(this QueueResultSegment qrs) {
         return (qrs == null) ? true : (qrs.SafeContinuationToken() != null);
      }
      /// <summary>Returns null if qrs is null or if its ContinuationToken is null.</summary>
      /// <param name="qrs">A QueueResultSegment.</param>
      /// <returns>A QueueContinuationToken.</returns>
      public static QueueContinuationToken SafeContinuationToken(this QueueResultSegment qrs) {
         return (qrs == null) ? null : qrs.ContinuationToken;
      }
   }
}

namespace Wintellect.Azure.Storage.Queue {
   /// <summary>A light-weight struct wrapping a CloudQueue. The methods exposed force the creation of scalable services.</summary>
   public struct AzureQueue {
      /// <summary>Implicitly converts an AzureQueue instance to a CloudQueue reference.</summary>
      /// <param name="aq">The AzureQueue instance.</param>
      /// <returns>The wrapped CloudQueue object reference.</returns>
      public static implicit operator CloudQueue(AzureQueue aq) { return aq.CloudQueue; }


      /// <summary>Implicitly converts a CloudQueue reference to an AzureQueue instance.</summary>
      /// <returns>A reference to a CloudQueue object.</returns>
      /// <param name="cq">A reference to the CloudQueue object.</param>
      /// <returns>An AzureQueue instance wrapping the CloudQueue object.</returns>
      public static implicit operator AzureQueue(CloudQueue cq) { return new AzureQueue(cq); }

      /// <summary>Initializes a new instance of the AzureQueue struct.</summary>
      /// <param name="cloudQueue">The CloudQueue that this structure wraps.</param>
      public AzureQueue(CloudQueue cloudQueue) { CloudQueue = cloudQueue; }

      /// <summary>Access to the wrapped CloudQueue.</summary>
      private readonly CloudQueue CloudQueue;

      /// <summary>Ensures the queue exists. Optionally deleting it first to clear it out.</summary>
      /// <param name="clear">Pass true to delete the queue first.</param>
      /// <returns>The same AzureQueue instance for fluent programming pattern.</returns>
      public async Task<AzureQueue> EnsureExistsAsync(Boolean clear = false) {
         await CreateIfNotExistsAsync().ConfigureAwait(false);
         if (clear) await ClearAsync().ConfigureAwait(false);
         return this;
      }

      /// <summary>Gets the approximate message count for the queue.</summary>
      public Int32? ApproximateMessageCount { get { return CloudQueue.ApproximateMessageCount; } }

      /// <summary>Gets or sets a value indicating whether to apply base64 encoding when adding or retrieving messages.</summary>
      public Boolean EncodeMessage { get { return CloudQueue.EncodeMessage; } set { CloudQueue.EncodeMessage = value; } }

      /// <summary>Gets the queue's metadata.</summary>
      public IDictionary<String, String> Metadata { get { return CloudQueue.Metadata; } }

      /// <summary>Gets the queue's name.</summary>
      public string Name { get { return CloudQueue.Name; } }

      /// <summary>Gets the service client for the queue.</summary>
      public CloudQueueClient ServiceClient { get { return CloudQueue.ServiceClient; } }

      /// <summary>Gets the queue's URI.</summary>
      public Uri Uri { get { return CloudQueue.Uri; } }

      /// <summary>
      /// Returns a task that performs an asynchronous operation to add a message to the queue.
      /// </summary>
      /// <param name="message">The message to add.</param>
      /// <param name="timeToLive">The maximum time to allow the message to be in the queue, or null.</param>
      /// <param name="initialVisibilityDelay">The length of time from now during which the message will be invisible. If null then the message will be visible immediately.</param>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Queue.QueueRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest]
      public Task AddMessageAsync(CloudQueueMessage message, TimeSpan? timeToLive = null, TimeSpan? initialVisibilityDelay = null, QueueRequestOptions options = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudQueue.AddMessageAsync(message, timeToLive, initialVisibilityDelay, options, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous operation to clear all messages from the queue.
      /// </summary>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Queue.QueueRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest]
      public Task ClearAsync(QueueRequestOptions options = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudQueue.ClearAsync(options, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous operation to create a queue.
      /// </summary>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Queue.QueueRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest]
      public Task CreateAsync(QueueRequestOptions options = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudQueue.CreateAsync(options, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous request to create the queue if it does not already exist.
      /// </summary>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Queue.QueueRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest]
      public Task<Boolean> CreateIfNotExistsAsync(QueueRequestOptions options = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudQueue.CreateIfNotExistsAsync(options, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous operation to delete a queue.
      /// </summary>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Queue.QueueRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest]
      public Task DeleteAsync(QueueRequestOptions options = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudQueue.DeleteAsync(options, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous request to delete the queue if it already exists.
      /// </summary>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Queue.QueueRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest]
      public Task<bool> DeleteIfExistsAsync(QueueRequestOptions options = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudQueue.DeleteIfExistsAsync(options, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous operation to delete a message.
      /// </summary>
      /// <param name="messageId">The message ID.</param>
      /// <param name="popReceipt">The pop receipt value.</param>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Queue.QueueRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest]
      public async Task<Boolean> DeleteMessageAsync(String messageId, String popReceipt, QueueRequestOptions options = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         try {
            await CloudQueue.DeleteMessageAsync(messageId, popReceipt, options, operationContext, cancellationToken).ConfigureAwait(false);
            return true;
         }
         catch (StorageException e) {
            if (!e.Matches(HttpStatusCode.NotFound, QueueErrorCodeStrings.MessageNotFound)) throw;
            // pop receipt must be invalid; ignore or log (so we can tune the visibility timeout)
            return false;
         }
      }

      /// <summary>
      /// Returns a task that performs an asynchronous operation to delete a message.
      /// </summary>
      /// <param name="message">The message.</param>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Queue.QueueRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      public Task<Boolean> DeleteMessageAsync(CloudQueueMessage message, QueueRequestOptions options = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return DeleteMessageAsync(message.Id, message.PopReceipt, options, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous request to check existence of the queue.
      /// </summary>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Queue.QueueRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest]
      public Task<Boolean> ExistsAsync(QueueRequestOptions options = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudQueue.ExistsAsync(options, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous operation to fetch the queue's attributes.
      /// </summary>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Queue.QueueRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest]
      public Task FetchAttributesAsync(QueueRequestOptions options = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudQueue.FetchAttributesAsync(options, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous operation to get a single message
      /// from the queue, and specifies how long the message should be reserved before
      /// it becomes visible, and therefore available for deletion.
      /// </summary>
      /// <param name="visibilityTimeout">The visibility timeout interval.</param>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Queue.QueueRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest]
      public Task<CloudQueueMessage> GetMessageAsync(TimeSpan? visibilityTimeout = null, QueueRequestOptions options = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudQueue.GetMessageAsync(visibilityTimeout, options, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous operation to get the specified
      /// number of messages from the queue using the specified request options and
      /// operation context. This operation marks the retrieved messages as invisible
      /// in the queue for the default visibility timeout period.
      /// </summary>
      /// <param name="messageCount">The number of messages to retrieve.</param>
      /// <param name="visibilityTimeout">The visibility timeout interval.</param>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Queue.QueueRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest]
      public Task<IEnumerable<CloudQueueMessage>> GetMessagesAsync(int messageCount, TimeSpan? visibilityTimeout = null, QueueRequestOptions options = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudQueue.GetMessagesAsync(messageCount, visibilityTimeout, options, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous request to get the permissions settings for the queue.
      /// </summary>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Queue.QueueRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest]
      public Task<QueuePermissions> GetPermissionsAsync(QueueRequestOptions options = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudQueue.GetPermissionsAsync(options, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a shared access signature for the queue.
      /// </summary>
      /// <param name="policy">The access policy for the shared access signature.</param>
      /// <param name="accessPolicyIdentifier">A queue-level access policy.</param>
      /// <returns>The query string returned includes the leading question mark.</returns>
      public string GetSharedAccessSignature(SharedAccessQueuePolicy policy, string accessPolicyIdentifier) {
         return CloudQueue.GetSharedAccessSignature(policy, accessPolicyIdentifier);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous operation to get a single message from the queue.
      /// </summary>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Queue.QueueRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest]
      public Task<CloudQueueMessage> PeekMessageAsync(QueueRequestOptions options = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudQueue.PeekMessageAsync(options, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous operation to peek messages from the queue.
      /// </summary>
      /// <param name="messageCount">The number of messages to peek.</param>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Queue.QueueRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest]
      public Task<IEnumerable<CloudQueueMessage>> PeekMessagesAsync(int messageCount, QueueRequestOptions options = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudQueue.PeekMessagesAsync(messageCount, options, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous operation to set user-defined metadata on the queue.
      /// </summary>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Queue.QueueRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest]
      public Task SetMetadataAsync(QueueRequestOptions options = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudQueue.SetMetadataAsync(options, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous request to set permissions for the queue.
      /// </summary>
      /// <param name="permissions">The permissions to apply to the queue.</param>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Queue.QueueRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest]
      public Task SetPermissionsAsync(QueuePermissions permissions, QueueRequestOptions options = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudQueue.SetPermissionsAsync(permissions, options, operationContext, cancellationToken);
      }

      /// <summary>
      /// Returns a task that performs an asynchronous operation to update the visibility timeout and optionally the content of a message.
      /// </summary>
      /// <param name="message">The message to update.</param>
      /// <param name="visibilityTimeout">The visibility timeout interval.</param>
      /// <param name="updateFields">An EnumSet of Microsoft.WindowsAzure.Storage.Queue.MessageUpdateFields values that specifies which parts of the message are to be updated.</param>
      /// <param name="options">A Microsoft.WindowsAzure.Storage.Queue.QueueRequestOptions object that specifies additional options for the request.</param>
      /// <param name="operationContext">An Microsoft.WindowsAzure.Storage.OperationContext object that represents the context for the current operation.</param>
      /// <param name="cancellationToken">A System.Threading.CancellationToken to observe while waiting for a task to complete.</param>
      /// <returns>A System.Threading.Tasks.Task object that represents the current operation.</returns>
      [DoesServiceRequest]
      public Task UpdateMessageAsync(CloudQueueMessage message, TimeSpan visibilityTimeout, MessageUpdateFields updateFields, QueueRequestOptions options = null, OperationContext operationContext = null, CancellationToken cancellationToken = default(CancellationToken)) {
         return CloudQueue.UpdateMessageAsync(message, visibilityTimeout, updateFields, options, operationContext, cancellationToken);
      }
      /// <summary>Returns a string that represents the current object.</summary>
      public override string ToString() { return CloudQueue.ToString(); }
      /// <summary>Determines whether the specified System.Object is equal to the current System.Object.</summary>
      /// <param name="obj">The object to compare with the current object.</param>
      /// <returns>true if the specified object is equal to the current object; otherwise, false.</returns>
      public override bool Equals(object obj) { return CloudQueue.Equals(obj); }
      /// <summary>Serves as a hash function for a particular type.</summary>
      /// <returns>A hash code for the current System.Object.</returns>
      public override int GetHashCode() { return CloudQueue.GetHashCode(); }
   }
}