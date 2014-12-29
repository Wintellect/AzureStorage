/******************************************************************************
Module:  StoragePatterns.cs
Notices: Copyright (c) by Jeffrey Richter & Wintellect
******************************************************************************/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Xml.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Blob.Protocol;
using Microsoft.WindowsAzure.Storage.File;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using Wintellect;
using Wintellect.Azure.Storage;
using Wintellect.Azure.Storage.Blob;
using Wintellect.Azure.Storage.Queue;
using Wintellect.Azure.Storage.Table;
using Wintellect.IO;

public static class AzureStoragePatterns {
   private static readonly Boolean c_SpawnStorageKiller = false;
   private static CloudStorageAccount m_account = null;
   private static CloudStorageAccount Account {
      [DebuggerStepThrough]
      get {
         if (m_account != null) return m_account;
         // Chose the default account to execute these demos against:
         m_account =
            GetStorageAccount(FiddlerPrompt(StorageAccountType.DevStorage, false));
         return m_account;
      }
   }
   private static void BatchLoggerDemo(CloudBlockBlob log) {
      using (var gs = new GatherStream(new[] { new Byte[3] })) {
         log.AppendAsync(gs, null).GetAwaiter().GetResult();
      }

      // Create the BatchLogger: it flushes every 1KB or every 3 seconds.
      var bl = new BatchLogger(1024, TimeSpan.FromSeconds(3), async byteArrays => {
         // This method is called when the BatchLogger is ready to persist the log data
         // Take the byteArrays and save them where ever you want.
         // OPTIONAL: To simplify things, you can use the GatherStream to convert the bytes arrays into a stream
         using (var gs = new GatherStream(byteArrays)) {
            await log.AppendAsync(gs, null);
         }
      });

      // Write some entries to the BatchLogger
      for (Int32 e = 0; e < 10; e++)
         bl.Append(Encoding.UTF8.GetBytes("Entry #" + e + Environment.NewLine));

      // If you want to reduce the change of losing log data, call this method to flush data more frequently.
      bl.EnableMinimalLoss();

      // Write more entries to the BatchLogger
      for (Int32 e = 10; e < 20; e++)
         bl.Append(Encoding.UTF8.GetBytes("Entry #" + e + Environment.NewLine));
   }
   private static DateTimeOffset? GetSslCertificateExpirationDate(String url = "https://WintellectNOW.com/") {
      var request = (HttpWebRequest)WebRequest.Create(url);
      var response = request.GetResponse();
      if (request.ServicePoint.Certificate != null) {
         return DateTimeOffset.Parse(request.ServicePoint.Certificate.GetExpirationDateString());
      }
      return null;
   }

   public static void Main() {
      //TablePatterns.CopyTableAsync(Account).Wait();
      //GamerManager.DemoAsync().GetAwaiter().GetResult();
      StorageAccountsEndpoints();

      BlobPatterns.BasicsAsync(Account).Wait();
      BlobPatterns.RootContainerAsync(Account).Wait();
      BlobPatterns.Attributes(Account).Wait();
      BlobPatterns.ConditionalOperationsAsync(Account).Wait();
      BlobPatterns.SharedAccessSignatures(Account).Wait();
      BlobPatterns.BlockBlobsAsync(Account).Wait();
      BlobPatterns.PageBlobs(Account).Wait();
      BlobPatterns.Snapshots(Account).Wait();
      BlobPatterns.DirectoryHierarchies(Account).Wait();
      BlobPatterns.Segmented(Account).Wait();

      FilePatterns.Basics(Account).Wait();

      TablePatterns.Basics(Account).Wait();
      TablePatterns.EnumeratePartitionKeys(Account).Wait();
      TablePatterns.OptimisticConcurrency(Account).Wait();
      TablePatterns.LastWriterWins(Account).Wait();
      TablePatterns.SignedAccessSignatures(Account).Wait();
      TablePatterns.QueryFilterStrings(Account);
      TablePatterns.SegmentedAsync(Account).Wait();
      TablePatterns.EntityGroupTransactionAsync(Account).Wait();
      TablePatterns.PropertyChanger(Account).Wait();

      QueuePatterns.Basics(Account).Wait();
      QueuePatterns.Idempotency.Demo(Account).Wait();
      QueuePatterns.Segmented(Account);

      StorageExtensions.ImprovePerformance(null, 100);
      //Management.Rest();
      var expiration = GetSslCertificateExpirationDate("https://WintellectNOW.com/");
   }

   [DebuggerStepThrough]
   private static StorageAccountType FiddlerPrompt(StorageAccountType accountType, Boolean prompt = true) {
      const MessageBoxOptions MB_TOPMOST = (MessageBoxOptions)0x00040000;
      if (prompt && (accountType == StorageAccountType.DevStorage)) {
         if (MessageBox.Show(
          "Drag Fiddler's Process Filter cursor on this window.\n" +
          "Are you using Fiddler?",
          "Wintellect's Windows Azure Data Storage Demo",
          MessageBoxButtons.YesNo, MessageBoxIcon.Information,
          MessageBoxDefaultButton.Button1, MB_TOPMOST) == DialogResult.Yes)
            accountType = StorageAccountType.DevStorageWithFiddler;
      }
      return accountType;
   }

   public enum StorageAccountType {
      AzureStorage,
      DevStorage,
      DevStorageWithFiddler
   }

   [DebuggerStepThrough]
   private static CloudStorageAccount GetStorageAccount(StorageAccountType accountType) {
      switch (accountType) {
         default:
         case StorageAccountType.DevStorage:
            return CloudStorageAccount.DevelopmentStorageAccount;
         case StorageAccountType.DevStorageWithFiddler:
            return CloudStorageAccount.Parse("UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://ipv4.fiddler");
         case StorageAccountType.AzureStorage:
            String accountName = "AzureSecrets.StorageAccountName";
            String accountKey = "AzureSecrets.StorageAccountKey";
            return new CloudStorageAccount(new StorageCredentials(accountName, accountKey), false);
      }
   }

   private static void StorageAccountsEndpoints() {
      CLS();

      Show("Azure storage endpoints:");
      String cxn =
         "AccountName=jeffrichter" +
         ";AccountKey=RQDC/YZ17F3uUf4nVcxun5ARn86jrAWXxuxs5kX0vSnOIj2Qrr42XqKaL3PIa9+rLxo8swr34CY9nxw/4DGE/g==" +
         ";DefaultEndpointsProtocol=https";
      CloudStorageAccount account = CloudStorageAccount.Parse(cxn);
      Show("   BlobEndpoint:  " + account.BlobEndpoint);
      Show("   FileEndpoint:  " + account.FileEndpoint);
      Show("   TableEndpoint: " + account.TableEndpoint);
      Show("   QueueEndpoint: " + account.QueueEndpoint).NL();

      Show("Storage emulator endpoints:");
      account = CloudStorageAccount.Parse("UseDevelopmentStorage=true");
      // Same as: account = CloudStorageAccount.DevelopmentStorageAccount;
      Show("   BlobEndpoint:  " + account.BlobEndpoint);
      Show("   FileEndpoint:  " + account.FileEndpoint);
      Show("   TableEndpoint: " + account.TableEndpoint);
      Show("   QueueEndpoint: " + account.QueueEndpoint).NL();
   }

   private static class Management {
      public static void Rest() {
         // Find the certificate from the thumbprint
         // Install PFX file on computer & upload CER file to Azure Management Certificates
         String thumbprint = "AzureSecrets.ManagementCertificateThumbprint";
         var store = new X509Store(StoreName.My, StoreLocation.CurrentUser);
         store.Open(OpenFlags.ReadOnly);
         X509Certificate2 cert = store.Certificates.OfType<X509Certificate2>().FirstOrDefault(
            c => String.Equals(c.Thumbprint, thumbprint, StringComparison.OrdinalIgnoreCase));
         store.Close();
         if (cert == null) return;

         String subscriptionId = "AzureSecrets.ManagementSubscriptionId";
         String serviceName = "AzureSecrets.StorageAccountName";
         var req = (HttpWebRequest)
            WebRequest.Create(String.Format("https://management.core.windows.net/{0}/services/storageservices/{1}",
            subscriptionId, serviceName));
         req.Headers.Add("x-ms-version", "2009-10-01");
         req.ClientCertificates.Add(cert);
         using (var response = req.GetResponse()) {
            Show(XElement.Load(response.GetResponseStream()));
         }
      }
   }

   private static class BlobPatterns {
      public static async Task BasicsAsync(CloudStorageAccount account) {
         CLS();
         CloudBlobClient client = account.CreateCloudBlobClient();

         // Create a container:
         CloudBlobContainer container = client.GetContainerReference("demo");
         Boolean created = await container.CreateIfNotExistsAsync();

         // Create 2 blobs in the container:
         CloudBlockBlob blob = container.GetBlockBlobReference("BlobOne.txt");
         using (var stream = new MemoryStream(("Some data created at " + DateTime.Now).Encode())) {
            blob.Properties.ContentType = "text/plain";
            await blob.UploadFromStreamAsync(stream);
         }
         using (var stream = new MemoryStream()) {
            await blob.DownloadToStreamAsync(stream);
            stream.Seek(0, SeekOrigin.Begin);
            Show(new StreamReader(stream).ReadToEnd());   // Read the blob data back
         }

         blob = container.GetBlockBlobReference("BlobTwo.txt");
         using (var stream = new MemoryStream(("Some more data created at " + DateTime.Now).Encode())) {
            blob.Properties.ContentType = "text/plain";
            await blob.UploadFromStreamAsync(stream);
         }

         // Attempt to access a blob from browser (fails due to security):
         ShowIE(blob.Uri);

         // Attempt to access the blob anonymously in code:
         CloudBlobClient anonymous = new CloudBlobClient(client.BaseUri);
         var credentials = anonymous.Credentials;  // Prove we have no credentials
         try {
            using (var stream = new MemoryStream()) {
               await anonymous.GetContainerReference("demo").GetBlockBlobReference("BlobOne.txt")
                  .DownloadToStreamAsync(stream);
               Show("Download result: ", stream.GetBuffer().Decode()).NL();
            }
         }
         catch (StorageException se) {
            Show(se.Message);
            if ((HttpStatusCode)se.RequestInformation.HttpStatusCode != HttpStatusCode.NotFound) throw;
         }

         // Change container's security to allow read access to its blobs:
         BlobContainerPermissions permissions = new BlobContainerPermissions {
            PublicAccess = BlobContainerPublicAccessType.Container
         };
         await container.SetPermissionsAsync(permissions);

         // Attempt to access a blob from browser & in code (succeeds):
         ShowIE(blob.Uri);
         using (var stream = new MemoryStream()) {
            await anonymous.GetContainerReference("demo").GetBlockBlobReference("BlobOne.txt")
               .DownloadToStreamAsync(stream);
            Show("Download result: " + stream.GetBuffer().Decode()).NL();
         }

         // Show the container's blobs via REST:
         ShowIE(container.Uri + "?comp=list");

         // Delete the container:
         await container.DeleteAsync();
      }

      public static async Task RootContainerAsync(CloudStorageAccount account) {
         CLS();
         CloudBlobClient client = account.CreateCloudBlobClient();

         // Create $root container, set permissions, put blob in it, access without $root:
         CloudBlobContainer container = await client.GetContainerReference("$root").EnsureExistsAsync();
         await container.SetPermissionsAsync(new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Container });
         CloudBlockBlob blob = container.GetBlockBlobReference("RootData.txt");
         var b = client.GetRootContainerReference().GetBlockBlobReference("RootData.txt").Uri.Equals(blob.Uri);
         await blob.UploadTextAsync("This data is in a root blob");
         ShowIE(blob.Uri);    // With $root

         String rootBlobUri = blob.Uri.ToString().Replace("$root/", String.Empty);
         ShowIE(rootBlobUri); // Without $root

         await container.DeleteAsync();
      }

      public static async Task Attributes(CloudStorageAccount account) {
         CLS();
         // Create a blob and attach some metadata to it:
         CloudBlobClient client = account.CreateCloudBlobClient();
         CloudBlobContainer container = await client.GetContainerReference("demo").EnsureExistsAsync();
         await container.SetPermissionsAsync(new BlobContainerPermissions() { PublicAccess = BlobContainerPublicAccessType.Container });
         CloudBlockBlob blob = container.GetBlockBlobReference("ReadMe.txt");
         await blob.UploadTextAsync("This is some text");
         blob.Metadata["CreatedBy"] = "Jeffrey";
         blob.Metadata["SourceMachine"] = Environment.MachineName;
         await blob.SetMetadataAsync();
         // NOTE: SetMetadata & SetProperties update the blob's ETag & LastModifiedUtc

         // Get blobs in container showing each blob's properties & metadata
         // See http://msdn.microsoft.com/en-us/library/dd135734.aspx for more options
         ShowIE(container.Uri + "?restype=container&comp=list&include=metadata");

         // This is the same as above but in code:
         for (BlobResultSegment rs = null; rs.HasMore(); ) {
            rs = await container.ListBlobsSegmentedAsync(null, false,
               BlobListingDetails.Metadata, null,
               rs.SafeContinuationToken(), null, null);
            foreach (IListBlobItem blobItem in rs.Results) { }
         }

         // Read the blob's attributes (which includes properties & metadata)
         await blob.FetchAttributesAsync();
         BlobProperties p = blob.Properties;
         Show("Blob's metadata (LastModified={0}, ETag={1})", p.LastModified, p.ETag);
         Show("   Content Type={0}, Encoding={1}, Language={2}, MD5={3}",
            p.ContentType, p.ContentEncoding, p.ContentLanguage, p.ContentMD5).NL();
         foreach (var kvp in blob.Metadata)
            Show("   {0} = {1}", kvp.Key, kvp.Value);
      }

      public static async Task SharedAccessSignatures(CloudStorageAccount account) {
         CLS();
         // Create a container (with no access) & upload a blob
         CloudBlobContainer container = await account.CreateCloudBlobClient()
            .GetContainerReference("sasdemo").EnsureExistsAsync();
         CloudBlockBlob blob = container.GetBlockBlobReference("Test.txt");
         await blob.UploadTextAsync("Data accessed!");
         ShowIE(blob.Uri); // This fails

         // Create a SAS for the blob
         var start = DateTime.UtcNow.AddMinutes(-5); // -5 for clock skew
         SharedAccessBlobPolicy sabp = new SharedAccessBlobPolicy {
            SharedAccessStartTime = start,
            SharedAccessExpiryTime = start.AddHours(1),
            Permissions = SharedAccessBlobPermissions.Read |
                          SharedAccessBlobPermissions.Write |
                          SharedAccessBlobPermissions.Delete
         };
         String sas = blob.GetSharedAccessSignature(sabp);
         String sasUri = blob.Uri + sas;
         ShowIE(sasUri); // This succeeds (in IE, modify URL and show failure)

         // Here's how we read the blob in code data via an SAS URI:
         CloudBlockBlob sasBlob = new CloudBlockBlob(blob.Uri, new StorageCredentials(sas));
         Show(await sasBlob.DownloadTextAsync()); // This succeeds

         // Alternatively, we can add the policy to the container with a name:
         String signatureIdentifier = "HumanResources";
         var permissions = await container.GetPermissionsAsync();
         // NOTE: A container can have up to 5 policies
         permissions.SharedAccessPolicies.Add(signatureIdentifier,
            new SharedAccessBlobPolicy {
               SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddYears(10),
               Permissions = SharedAccessBlobPermissions.Read |
                             SharedAccessBlobPermissions.Write
            });
         await container.SetPermissionsAsync(permissions);

         // This SharedAccessPolicy CAN'T specify what is already present in the container's policy
         sas = blob.GetSharedAccessSignature(new SharedAccessBlobPolicy { }, signatureIdentifier);
         sasUri = blob.Uri + sas;
         ShowIE(sasUri);

         // We can now revoke access on the container:
         permissions.SharedAccessPolicies.Remove(signatureIdentifier);
         await container.SetPermissionsAsync(permissions);
         ShowIE(sasUri); // This fails now
      }

#if false
         // No retry for 306 (Unused), 4xx, 501 (NotImplemented), 505 (HttpVersionNotSupported)
         client.RetryPolicy = new ExponentialRetry();

         // Time server can process a request (default = 90 secs)
         client.ServerTimeout = TimeSpan.FromSeconds(90);

         // Time client can wait for response across all retries (default = disabled)
         client.MaximumExecutionTime = TimeSpan.FromSeconds(5);
#endif

      public static async Task ConditionalOperationsAsync(CloudStorageAccount account) {
         CLS();
         // Create a blob and attach some metadata to it:
         CloudBlobClient client = account.CreateCloudBlobClient();
         CloudBlobContainer container = await client.GetContainerReference("demo").EnsureExistsAsync();
         CloudBlockBlob blob = container.GetBlockBlobReference("Data.txt");
         await blob.UploadTextAsync("Data");

         // Download blob content if newer than what we have (fails):
         try {
            await blob.DownloadTextAsync(Encoding.UTF8,
               AccessCondition.GenerateIfModifiedSinceCondition(blob.Properties.LastModified.Value),
               null, null);
         }
         catch (StorageException ex) {
            Show(String.Format("Failure: Status={0}({0:D}), Msg={1}",
               (HttpStatusCode)ex.RequestInformation.HttpStatusCode,
               ex.RequestInformation.HttpStatusMessage));
         }

         // Download blob content if more than 1 day old (fails):
         try {
            await blob.DownloadTextAsync(Encoding.UTF8,
               AccessCondition.GenerateIfNotModifiedSinceCondition(DateTimeOffset.UtcNow.AddDays(-1)),
               null, null);
         }
         catch (StorageException ex) {
            Show(String.Format("Failure: Status={0}({0:D}), Msg={1}",
               (HttpStatusCode)ex.RequestInformation.HttpStatusCode,
               ex.RequestInformation.HttpStatusMessage));
         }

         // Upload new content if the blob wasn't changed behind our back (succeeds):
         try {
            await blob.UploadTextAsync("Succeeds",
                     Encoding.UTF8, AccessCondition.GenerateIfMatchCondition(blob.Properties.ETag),
                     null, null);
         }
         catch (StorageException ex) {
            Show(String.Format("Failure: Status={0}({0:D}), Msg={1}",
               (HttpStatusCode)ex.RequestInformation.HttpStatusCode,
               ex.RequestInformation.HttpStatusMessage));
         }


         // Download content if it changed behind our back (fails):
         try {
            await blob.DownloadTextAsync(Encoding.UTF8,
               AccessCondition.GenerateIfNoneMatchCondition(blob.Properties.ETag), null, null);
         }
         catch (StorageException ex) {
            Show(String.Format("Failure: Status={0}({0:D}), Msg={1}",
               (HttpStatusCode)ex.RequestInformation.HttpStatusCode,
               ex.RequestInformation.HttpStatusMessage));
         }

         // Upload our content if it doesn't already exist (fails):
         try {
            await blob.UploadTextAsync("Fails", Encoding.UTF8,
               AccessCondition.GenerateIfNoneMatchCondition("*"),
               null, null);
         }
         catch (StorageException ex) {
            Show(String.Format("Failure: Status={0}({0:D}), Msg={1}",
               (HttpStatusCode)ex.RequestInformation.HttpStatusCode,
               ex.RequestInformation.HttpStatusMessage));
         }
      }

#if false
      private static async Task<String> Try(Func<Task<String>> func) {
         try { Show(await func()); }
         catch (StorageException ex) {
            Show(String.Format("Failure: Status={0}({0:D}), Msg={1}",
               (HttpStatusCode)ex.RequestInformation.HttpStatusCode,
               ex.RequestInformation.HttpStatusMessage));
         }
      }

      private static async Task Try(Func<Task> func) {
         Try(() => { await func(); return "Success"; });
      }
#endif
      /* The client library can automatically upload large blobs in blocks:
      // 1. Set # of blocks to simultaneously upload (range=1-64, default=1)
      client.ParallelOperationThreadCount = Environment.ProcessorCount;

      // 2. Define size of "large blob" (range=1MB-64MB, default=32MB)
      client.SingleBlobUploadThresholdInBytes = 32 * 1024 * 1024;

      // 3. Set individual block size (range=16KB-4MB, default=4MB)
      blockBlob.StreamWriteSizeInBytes = 4 * 1024 * 1024; */

      public static async Task BlockBlobsAsync(CloudStorageAccount account) {
         CLS();
         CloudBlobClient client = account.CreateCloudBlobClient();
         CloudBlobContainer container = await client.GetContainerReference("demo").EnsureExistsAsync();
         CloudBlockBlob blockBlob = container.GetBlockBlobReference("MyBlockBlob.txt");

         // Put 3 blocks to the blob verifying data integrity
         String[] words = new[] { "A ", "B ", "C " };
         var MD5 = new MD5Cng();
         for (Int32 word = 0; word < words.Length; word++) {
            Byte[] wordBytes = words[word].Encode(Encoding.UTF8);

            // Azure verifies data integrity during transport; failure=400 (Bad Request)
            String md5Hash = MD5.ComputeHash(wordBytes).ToBase64String();
            await blockBlob.PutBlockAsync(word.ToBlockId(), new MemoryStream(wordBytes), md5Hash);
         }

         Show("Blob's uncommitted blocks:");
         foreach (ListBlockItem lbi in await blockBlob.DownloadBlockListAsync(BlockListingFilter.Uncommitted, null, null, null))
            Show("   Name={0}, Length={1}, Committed={2}",
               lbi.Name.FromBlockId(), lbi.Length, lbi.Committed);
         try {
            Show(await blockBlob.DownloadTextAsync());   // Fails
         }
         catch (StorageException ex) {
            Show(String.Format("Failure: Status={0}({0:D}), Msg={1}",
               (HttpStatusCode)ex.RequestInformation.HttpStatusCode,
               ex.RequestInformation.HttpStatusMessage));
         }

         // Commit the blocks in order (and multiple times):
         await blockBlob.PutBlockListAsync(
            new[] { 0.ToBlockId(), 0.ToBlockId(), 1.ToBlockId(), 2.ToBlockId() });
         Show(await blockBlob.DownloadTextAsync());   // Succeeds

         Show("Blob's committed blocks:");
         foreach (ListBlockItem lbi in await blockBlob.DownloadBlockListAsync())
            Show("   Name={0}, Length={1}, Committed={2}",
               lbi.Name.FromBlockId(), lbi.Length, lbi.Committed);

         // You can change the block order & remove a block:
         await blockBlob.PutBlockListAsync(new[] { 2.ToBlockId(), 1.ToBlockId() });
         Show(await blockBlob.DownloadTextAsync());   // Succeeds

         // 2-day Log example:
         CloudBlockBlob log = Account.CreateCloudBlobClient().GetContainerReference("demo").GetBlockBlobReference("Log.txt");
         const Int32 maxDays = 2;
         DateTimeOffset date = DateTimeOffset.UtcNow;
         await AppendBlockAsync(log, date.ToString("yyyyMMdd"), "1st day's log data\r\n", maxDays);
         Show(await log.DownloadTextAsync());

         date = date.AddDays(1);
         await AppendBlockAsync(log, date.ToString("yyyyMMdd"), "2nd day's log data\r\n", maxDays);
         Show(await log.DownloadTextAsync());

         date = date.AddDays(1);
         await AppendBlockAsync(log, date.ToString("yyyyMMdd"), "3rd day's log data\r\n", maxDays);
         Show(await log.DownloadTextAsync());
      }

      private static async Task AppendBlockAsync(CloudBlockBlob blob, String blockId, String text, Int32 maxBlocks = 50000) {
         blockId = blockId.Encode().ToBase64String();

         // Get the blob's current block list
         List<String> blockList = null;
         try { blockList = (await blob.DownloadBlockListAsync()).Select(lbi => lbi.Name).ToList(); }
         catch (StorageException ex) {
            if (!ex.Matches(HttpStatusCode.NotFound)) throw;
            blockList = new List<String>();
         }

         // Upload the new block & add it's ID to the block list 
         await blob.PutBlockAsync(blockId, new MemoryStream(text.Encode()), null);
         blockList.Add(blockId);

         // Upload the new block list (deleting any earlier blocks if > maxBlocks)
         Int32 start = (blockList.Count <= maxBlocks) ? 0 : blockList.Count - maxBlocks;
         await blob.PutBlockListAsync(blockList.Skip(start));
      }

      private sealed class RecordPageBlob {
         const Int32 c_BlobPageSize = 512;
         private readonly CloudPageBlob m_blob;
         private readonly Int32 m_bytesPerRecord;
         public RecordPageBlob(CloudPageBlob pageBlob, Int32 bytesPerRecord) {
            m_blob = pageBlob;
            Int32 pages = bytesPerRecord / c_BlobPageSize;
            if ((bytesPerRecord % c_BlobPageSize) > 0) pages += 1;
            m_bytesPerRecord = pages * c_BlobPageSize;
         }
         public Task SetMinimumAsync(Int32 numRecords) {
            return AzureTable.OptimisticRetryAsync(async () => {
               Int32 size = numRecords * m_bytesPerRecord;
            Resize:
               try {
                  // Get the blob's current size
                  await m_blob.FetchAttributesAsync();
                  if (m_blob.Properties.Length < size) {
                     // The blob is smaller than desired, make it bigger
                     await m_blob.ResizeAsync(size,
                        AccessCondition.GenerateIfMatchCondition(m_blob.Properties.ETag),
                        null, null);
                  }
                  return;
               }
               catch (StorageException ex) {
                  if (!ex.Matches(HttpStatusCode.NotFound/*, BlobErrorCodeStrings.BlobNotFound*/)) throw;
               }
               try {
                  await m_blob.CreateAsync(size,
                     AccessCondition.GenerateIfNoneMatchCondition("*"), null, null);
                  return; // Sucessfully created blob
               }
               catch (StorageException ex) {
                  // Other PC created blob behind our back
                  if (!ex.Matches(HttpStatusCode.Conflict, BlobErrorCodeStrings.BlobAlreadyExists)) throw;
               }
               goto Resize;
            });
         }
         public async Task WriteAsync(Int32 recordNum, Byte[] recordData) {
            if (recordData.Length != m_bytesPerRecord) Array.Resize(ref recordData, m_bytesPerRecord);
            await m_blob.WritePagesAsync(new MemoryStream(recordData), recordNum * m_bytesPerRecord, null);
         }
         public async Task<Byte[]> ReadAsync(Int32 recordNum, Byte[] data = null) {
            data = data ?? new Byte[m_bytesPerRecord];
            await m_blob.DownloadRangeToByteArrayAsync(data, 0, recordNum * m_bytesPerRecord, m_bytesPerRecord);
            return data;
         }
         public Task ClearAsync(Int32 startRecordNum, Int32 numRecords = 1) {
            return m_blob.ClearPagesAsync(startRecordNum * m_bytesPerRecord, numRecords * m_bytesPerRecord);
         }
         public async Task<IEnumerable<Int64>> GetRecordsAsync() {
            IEnumerable<PageRange> ranges = await m_blob.GetPageRangesAsync();
            var records = new List<Int64>();
            foreach (PageRange pr in ranges) {
               // If records always written, then the start offset should always be on a record boundary
               Debug.Assert(pr.StartOffset % m_bytesPerRecord == 0);
               for (Int64 recOffset = pr.StartOffset; recOffset < pr.EndOffset; recOffset += m_bytesPerRecord)
                  records.Add(recOffset / m_bytesPerRecord);
            }
            return records;
         }
      }

      public static async Task PageBlobs(CloudStorageAccount account) {
         CLS();
         CloudBlobClient client = account.CreateCloudBlobClient();
         CloudBlobContainer container = await client.GetContainerReference("demo").EnsureExistsAsync();
         CloudPageBlob pageBlob = container.GetPageBlobReference("MyPageBlob.txt");
         await pageBlob.DeleteIfExistsAsync();
         RecordPageBlob rpb = new RecordPageBlob(pageBlob, 1024); // Each record is 1KB
         await rpb.SetMinimumAsync(10);

         // Write 4 days of data (the day is the index):
         await rpb.WriteAsync(0, "Day 0 data".Encode());
         await rpb.WriteAsync(1, "Day 1 data".Encode());
         await rpb.WriteAsync(2, "Day 2 data".Encode());
         await rpb.WriteAsync(3, "Day 3 data".Encode());

         // Delete day 1's data:
         await rpb.ClearAsync(1, 2);

         // Show committed pages:
         foreach (PageRange pr in await pageBlob.GetPageRangesAsync())
            Show("Start={0,6:N0}, End={1,6:N0}", pr.StartOffset, pr.EndOffset);

         // Read day 1's data (all zeros; empty string):
         String data = (await rpb.ReadAsync(1)).Decode().TrimEnd((Char)0);

         // Read day 2's data (works):
         data = (await rpb.ReadAsync(2)).Decode().TrimEnd((Char)0);

         await pageBlob.DeleteAsync();
      }

      public static async Task Segmented(CloudStorageAccount account) {
         CLS();
         CloudBlobClient client = account.CreateCloudBlobClient();

         // ListContainers/BlobsSegmented return up to 5,000 items
         const Int32 desiredItems = 5000 + 1000;  // Try to read more than the max

         // Create a bunch of blobs (this takes a very long time):
         CloudBlobContainer container = client.GetContainerReference("manyblobs");
         await container.EnsureExistsAsync();
         for (Int32 i = 0; i < desiredItems; i++)
            await container.GetBlockBlobReference(String.Format("{0:00000}", i)).UploadTextAsync("");

         // Enumerate containers & blobs in segments
         for (ContainerResultSegment crs = null; crs.HasMore(); ) {
            crs = await client.ListContainersSegmentedAsync(null, ContainerListingDetails.None, desiredItems, crs.SafeContinuationToken(), null, null);
            foreach (var c in crs.Results) {
               Show("Container: " + c.Uri);
               for (BlobResultSegment brs = null; brs.HasMore(); ) {
                  brs = await c.ListBlobsSegmentedAsync(null, true, BlobListingDetails.None, desiredItems, brs.SafeContinuationToken(), null, null);
                  foreach (var b in brs.Results) Show("   Blob: " + b.Uri);
               }
            }
         }
      }

      public static async Task Snapshots(CloudStorageAccount account) {
         CLS();
         CloudBlobClient client = account.CreateCloudBlobClient();
         CloudBlobContainer container = await client.GetContainerReference("demo").EnsureExistsAsync(true);

         // Create the original blob:
         CloudBlockBlob origBlob = container.GetBlockBlobReference("Original.txt");
         await origBlob.UploadTextAsync("Original data");

         // Create a snapshot of the original blob & save its timestamp:
         CloudBlockBlob snapshotBlob = await origBlob.CreateSnapshotAsync();
         DateTimeOffset? snapshotTime = snapshotBlob.SnapshotTime;

         // Try to write to the snapshot blob:
         try { await snapshotBlob.UploadTextAsync("Fails"); }
         catch (InvalidOperationException ex) { Show(ex.Message); }

         // Modify the original blob & show it:
         await origBlob.UploadTextAsync("New data");
         Show(await origBlob.DownloadTextAsync()).NL();      // New data

         // Show snapshot blob via original blob URI & snapshot time:
         snapshotBlob = container.GetBlockBlobReference("Original.txt", snapshotTime);
         Show(await snapshotBlob.DownloadTextAsync());  // Original data

         // Show all blobs in the container with their snapshots:
         for (BlobResultSegment rs = null; rs.HasMore(); ) {
            rs = await container.ListBlobsSegmentedAsync(null, true,
               BlobListingDetails.Snapshots, null,
               rs.SafeContinuationToken(), null, null);
            foreach (CloudBlockBlob b in rs.Results) {
               Show("Uri={0}, Snapshot={1}", b.Uri, b.SnapshotTime);
            }
         }

         // Create writable blob from the snapshot:
         CloudBlockBlob writableBlob = container.GetBlockBlobReference("Copy.txt");
         await writableBlob.StartCopyFromBlobAsync(snapshotBlob);
         Show(await writableBlob.DownloadTextAsync());
         await writableBlob.UploadTextAsync("Success");
         Show(await writableBlob.DownloadTextAsync());

         // DeleteSnapshotsOption: None (blob only; throws StorageException if snapshots exist), IncludeSnapshots, DeleteSnapshotsOnly
         await origBlob.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots, null, null, null);
      }

      public static async Task DirectoryHierarchies(CloudStorageAccount account) {
         CLS();
         CloudBlobClient client = account.CreateCloudBlobClient();
         Show("Default delimiter={0}", client.DefaultDelimiter /* settable */).NL();

         // Create the virtual directory
         const String virtualDirName = "demo";
         CloudBlobContainer virtualDir = await client.GetContainerReference(virtualDirName).EnsureExistsAsync(true);

         // Create some file entries under the virtual directory
         String[] virtualFiles = new String[] {
            "FileA", "FileB", // Avoid  $&+,/:=?@ in blob names
            "Dir1/FileC", "Dir1/FileD", "Dir1/Dir2/FileE",
            "Dir3/FileF", "Dir3/FileG",
            "Dir4/FileH"
         };
         foreach (String file in virtualFiles) {
            await virtualDir.GetBlockBlobReference("Root/" + file).UploadTextAsync(String.Empty);
         }

         // Show the blobs in the virtual directory container
         await ShowContainerBlobsAsync(virtualDir);   // Same as UseFlatBlobListing = false
         Show();
         await ShowContainerBlobsAsync(virtualDir, true);

         // CloudBlobDirectory (derived from IListBlobItem) is for traversing 
         // and accessing blobs with names structured in a directory hierarchy.
         CloudBlobDirectory root = virtualDir.GetDirectoryReference("Root");
         await WalkBlobDirHierarchyAsync(root, 0);

         // Show just the blobs under Dir1
         Show();
         String subdir = virtualDir.Name + "/Root/Dir1/";
         foreach (var file in client.ListBlobs(subdir))
            Show(file.Uri);
      }

      private static async Task ShowContainerBlobsAsync(CloudBlobContainer container, Boolean useFlatBlobListing = false, BlobListingDetails details = BlobListingDetails.None, BlobRequestOptions options = null, OperationContext operationContext = null) {
         Show("Container: " + container.Name);
         for (BlobResultSegment brs = null; brs.HasMore(); ) {
            brs = await container.ListBlobsSegmentedAsync(null, useFlatBlobListing, details,
               1000, brs.SafeContinuationToken(), options, operationContext);
            foreach (var blob in brs.Results) Show("   " + blob.Uri);
         }
      }

      private static async Task WalkBlobDirHierarchyAsync(CloudBlobDirectory dir, Int32 indent) {
         // NOTE: This code does not scale well with lots of blobs
         IEnumerable<IListBlobItem> entries = Enumerable.Empty<IListBlobItem>();
         // Get all the entries in the root directory
         for (BlobResultSegment rs = null; rs.HasMore(); ) {
            rs = await dir.ListBlobsSegmentedAsync(rs.SafeContinuationToken());
            entries = entries.Concat(rs.Results);
         }
         String spaces = new String(' ', indent * 3);

         Show(spaces + dir.Prefix + " entries:");
         foreach (var entry in entries.OfType<ICloudBlob>())
            Show(spaces + "   " + entry.Name);

         foreach (var entry in entries.OfType<CloudBlobDirectory>()) {
            String[] segments = entry.Uri.Segments;
            CloudBlobDirectory subdir = dir.GetDirectoryReference(segments[segments.Length - 1]);
            await WalkBlobDirHierarchyAsync(subdir, indent + 1); // Recursive call
         }
      }

      private static async Task ShowContainerAsync(CloudBlobContainer container, Boolean showBlobs) {
         Show("Blob container={0}", container);

         BlobContainerPermissions permissions = await container.GetPermissionsAsync();
         String[] meanings = new String[] {
            "no public access",
            "anonymous clients can read container & blob data",
            "anonymous readers can read blob data only"
         };
         Show("Container's public access={0} ({1})",
            permissions.PublicAccess, meanings[(Int32)permissions.PublicAccess]);

         // Show collection of access policies; each consists of name & SharedAccesssPolicy
         // A SharedAccesssBlobPolicy contains:
         //    SharedAccessPermissions enum (None, Read, Write, Delete, List) & 
         //    SharedAccessStartTime/SharedAccessExpireTime
         Show("   Shared access policies:");
         foreach (var policy in permissions.SharedAccessPolicies) {
            Show("   {0}={1}", policy.Key, policy.Value);
         }

         await container.FetchAttributesAsync();
         Show("   Attributes: Name={0}, Uri={1}", container.Name, container.Uri);
         Show("   Properties: LastModified={0}, ETag={1},", container.Properties.LastModified, container.Properties.ETag);
         ShowMetadata(container.Metadata);

         if (showBlobs) {
            for (BlobResultSegment rs = null; rs.HasMore(); ) {
               rs = await container.ListBlobsSegmentedAsync(rs.SafeContinuationToken());
               foreach (ICloudBlob b in rs.Results) ShowBlob(b);
            }
         }
      }

      private static void ShowBlob(ICloudBlob blob) {
         // A blob has attributes: Uri, Snapshot DateTime?, Properties & Metadata
         // The CloudBlob Uri/SnapshotTime/Properties/Metadata properties return these
         // You can set the properties & metadata; not the Uri or snapshot time
         Show("Blob Uri={0}, Snapshot time={1}", blob.Uri, blob.SnapshotTime);
         BlobProperties bp = blob.Properties;
         Show("BlobType={0}, CacheControl={1}, Encoding={2}, Language={3}, MD5={4}, ContentType={5}, LastModified={6}, Length={7}, ETag={8}",
            bp.BlobType, bp.CacheControl, bp.ContentEncoding, bp.ContentLanguage, bp.ContentMD5, bp.ContentType, bp.LastModified, bp.Length, bp.ETag);
         ShowMetadata(blob.Metadata);
      }

      private static void ShowMetadata(IDictionary<String, String> metadata) {
         foreach (var kvp in metadata)
            Show("{0}={1}", kvp.Key, kvp.Value);
      }
   }

   private static class FilePatterns {
      public static async Task Basics(CloudStorageAccount account) {
         account = new CloudStorageAccount(new StorageCredentials("wintellect", "1EhT1QcbsPSlAsl73Dt6IjHBmFM52I8ZXr4INFXdHmVS7JJAbZq7TN62SrqmfPB3CByS9QBHVRI1gR08gDC5Yg=="), false);
         CLS();
         CloudFileClient client = account.CreateCloudFileClient();
         CloudFileShare share = client.GetShareReference("myshare");
         Boolean ok = await share.CreateIfNotExistsAsync();

         CloudFileDirectory root = share.GetRootDirectoryReference();
         ok = await root.CreateIfNotExistsAsync();

         // Put some files in the root:
         await root.GetFileReference("FileA").UploadTextAsync("FileA's data");
         await root.GetFileReference("FileB").UploadTextAsync("FileB's data");

         // Create a subdirectory & put a file in it
         CloudFileDirectory dir = root.GetDirectoryReference("SubDir");
         await dir.CreateIfNotExistsAsync();
         await dir.GetFileReference("FileC").UploadTextAsync("FileC's data");

         Show("Root:");
         foreach (CloudFileDirectory subdir in await ShowFiles(root)) {
            Show("Subdir: " + subdir.Name);
            await ShowFiles(subdir);
         }
         await share.DeleteAsync();
      }
      private static async Task<IEnumerable<CloudFileDirectory>> ShowFiles(CloudFileDirectory dir) {
         var subdirs = new List<CloudFileDirectory>();
         foreach (IListFileItem lfi in (await dir.ListFilesAndDirectoriesSegmentedAsync(null)).Results) {
            CloudFile file = lfi as CloudFile;
            if (file != null) {
               Show(file.Name + ": " + await file.DownloadTextAsync());
            } else {
               subdirs.Add((CloudFileDirectory)lfi);
            }
         }
         return subdirs;
      }
   }
   private static class TablePatterns {
      public static async Task Basics(CloudStorageAccount account) {
         CLS();
         CloudTableClient client = account.CreateCloudTableClient();
         AzureTable ct = client.GetTableReference("Basics");
         await ct.CreateIfNotExistsAsync();

         // Insert an entity into the table:
         var entity = new DynamicTableEntity("Jeff Richter", "C") {
            Properties = { { "Address", new EntityProperty("1 Main Street") } }
         };
         TableResult tr = await ct.ExecuteAsync(TableOperation.InsertOrMerge(entity));

         // Merge a property in with the existing entity
         entity = new DynamicTableEntity("Jeff Richter", "C") {
            Properties = { { "City", new EntityProperty("Seattle") } }
         };
         tr = await ct.ExecuteAsync(TableOperation.InsertOrMerge(entity));

         // Insert another entity into the table:
         entity = new DynamicTableEntity("Paul Mehner", "C") {
            Properties = {
               { "Address", new EntityProperty("123 Front Street") },
               { "City", new EntityProperty("New York") } }
         };
         tr = await ct.ExecuteAsync(TableOperation.InsertOrMerge(entity));

         // Show Query, Update & Delete operations
         var propName = new PropertyName<CustomerEntity>();
         foreach (var pk in new[] { "Jeff Richter", "Paul Mehner" }) {
            // Read all entities where PartitionKey == pk
            TableQuery query = new TableQuery {
               FilterString = new TableFilterBuilder<CustomerEntity>()
                  .And(ce => ce.PartitionKey, CompareOp.EQ, pk),
               SelectColumns = propName[ce => ce.Address, ce => ce.City],
               TakeCount = null
            };

            for (var tqc = ct.CreateQueryChunker(query); tqc.HasMore; ) {
               foreach (DynamicTableEntity dte in await tqc.TakeAsync()) {
                  String city = dte["City"].StringValue;
                  if (city == "Seattle") { // Update "Jeff Richter"
                     dte["City"] = new EntityProperty(city.ToUpper());
                     tr = await ct.ExecuteAsync(TableOperation.Merge(dte));
                  } else { // Delete "Paul"
                     tr = await ct.ExecuteAsync(TableOperation.Delete(dte));
                  }
               }
            }
         }
      }

#if false
         // Using a resolver to fetch a single property value for all entities
         EntityResolver<String> er = (String partitionKey, String rowKey, DateTimeOffset timestamp, IDictionary<String, EntityProperty> properties, String etag) => {
            EntityProperty entityProperty;
            return properties.TryGetValue(new PropertyName<CustomerEntity>(ce => ce.Name), out entityProperty) ? entityProperty.StringValue : null;
         };

         foreach (var name in ct.ExecuteQuery(new TableQuery<CustomerEntity>(), er)) {
            Show(name);
         }
#endif

      public static async Task EnumeratePartitionKeys(CloudStorageAccount account) {
         AzureTable ct = account.CreateCloudTableClient().GetTableReference("PartitionsTable");
         await ct.EnsureExistsAsync(true);

         for (Int32 pk = 0; pk <= 5; pk++) {
            for (Int32 rk = 0; rk < 5; rk++) {
               await ct.ExecuteAsync(TableOperation.Insert(new DynamicTableEntity(pk.ToString(), rk.ToString())));
            }
         }
         var tq = new TableQuery { TakeCount = 1 };
         for (Boolean done = false; !done; ) {
            for (var tqc = ct.CreateQueryChunker(tq); tqc.HasMore; ) {
               var dte = (await tqc.TakeAsync(1)).FirstOrDefault();
               if (dte == null) { done = true; break; } // No more partitions
               Console.WriteLine(dte.PartitionKey);
               tq.FilterString = new TableFilterBuilder<TableEntity>()
                  .And(te => te.PartitionKey, CompareOp.GT, dte.PartitionKey);
            }
         }
      }

      // NOTE: This method doesn't actually run the query filter strings it creates
      public static void QueryFilterStrings(CloudStorageAccount account) {
         CLS();

         CloudTable ct = account.CreateCloudTableClient().GetTableReference("Table");
         const String pk = "PK", rk = "RK";
#if LinqFailures || false
         ct.CreateIfNotExists();
         var dte = new DynamicTableEntity("ABC", String.Empty);
         ct.Execute(TableOperation.InsertOrReplace(dte));

         dte = new DynamicTableEntity("abc", String.Empty);
         ct.Execute(TableOperation.InsertOrReplace(dte));
         // NOTE: This returns ZERO results:
         var results = (from e in ct.CreateQuery<DynamicTableEntity>()
                        where String.Compare(e.PartitionKey, "Abc", StringComparison.OrdinalIgnoreCase) == 0
                        select e).ToArray();
         // NOTE: This throws ArgumentException:
         results = (from e in ct.CreateQuery<DynamicTableEntity>()
                    where String.Equals(e.PartitionKey, "Abc", StringComparison.OrdinalIgnoreCase)
                    select e).ToArray();
#endif

         // Best performance: Query single entity via specific PK/RK
         // This syntax puts the PK & RK in the URL itself
         var tfb = new TableFilterBuilder<TableEntity>();
         var q1 = new TableQuery {
            FilterString = tfb.And(te => te.PartitionKey, CompareOp.EQ, pk).And(te => te.RowKey, CompareOp.EQ, rk)
         };
         Show(q1.FilterString);
         // Similar to: ((CloudTable)null).Execute(TableOperation.Retrieve<TableEntity>(pk, rk));

         var q2 = new TableQuery {
            FilterString = tfb.And(te => te.PartitionKey, CompareOp.EQ, pk)
         };
         Show(q2.FilterString);

         // Worst performance: Query against whole table
         var q3 = new TableQuery { FilterString = String.Empty };
         Show(q3.FilterString);

         // Medium performance: Query against discrete partitions in parallel
         // I perform the queries sequentially here; normally you'd do them in parallel
         // NOTE: NextKey is my extension method
         var q4 = new TableQuery { FilterString = tfb.And(te => te.PartitionKey, pk) };
         Show(q4.FilterString);

         var s = TableQuery.GenerateFilterConditionForBool("ABoolean", "eq", true);
         s = TableQuery.GenerateFilterConditionForBinary("AByteArray", "eq", new Byte[] { 1, 2, 3, 4, 5 });
         s = TableQuery.GenerateFilterConditionForDate("ADate", "eq", DateTimeOffset.Now);
         s = TableQuery.GenerateFilterConditionForDouble("ADouble", "eq", 1.23);
         s = TableQuery.GenerateFilterConditionForGuid("AGuid", "eq", Guid.NewGuid());
         s = TableQuery.GenerateFilterConditionForInt("AnInt32", "eq", 123);
         s = TableQuery.GenerateFilterConditionForLong("AnInt64", "eq", 321);

         var q5 = new TableFilterBuilder<MyTableEntity>()
            .And(te => te.PartitionKey, "Jeff")
            .And(te => te.ABoolean, EqualOp.EQ, true)
            .And(te => te.AByteArray, EqualOp.EQ, new Byte[] { 1, 2, 3 })
            .And(te => te.ADate, CompareOp.GT, new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero))
            .And(te => te.ADouble, CompareOp.LT, 1.23)
            .And(te => te.AGuid, EqualOp.EQ, Guid.NewGuid())
            .And(te => te.AnInt32, CompareOp.GE, 123)
            .And(te => te.AnInt64, CompareOp.NE, 321);

         var tq = new TableQuery {
            FilterString = new TableFilterBuilder<MyTableEntity>()
               .And(te => te.ABoolean, EqualOp.EQ, true)
               .And(te => te.AByteArray, EqualOp.EQ, new Byte[] { 1, 2, 3, 4, 5 })
               .And(te => te.ADate, CompareOp.EQ, DateTimeOffset.UtcNow)
               .And(te => te.ADouble, CompareOp.LT, 1.23)
               .And(te => te.AGuid, EqualOp.EQ, Guid.NewGuid())
               .And(te => te.AnInt32, CompareOp.GE, 123)
               .And(te => te.AnInt64, CompareOp.NE, 321)
         };
         //var segment = await ct.ExecuteQuerySegmentedAsync(tq, null, null, null, CancellationToken.None);
      }

      private sealed class MyTableEntity : TableEntityBase {
         public MyTableEntity() : base(null) { }
         public Boolean ABoolean { get; set; }
         public Byte[] AByteArray { get; set; }
         public DateTimeOffset ADate { get; set; }
         public Double ADouble { get; set; }
         public Guid AGuid { get; set; }
         public Int32 AnInt32 { get; set; }
         public Int64 AnInt64 { get; set; }
      }

      public static async Task OptimisticConcurrency(CloudStorageAccount account) {
         CLS();
         // Preparation: Clear the table and add a customer to it
         CloudTableClient client = account.CreateCloudTableClient();
         AzureTable ct = await ((AzureTable)client.GetTableReference("Concurrency")).EnsureExistsAsync(true);
         var ce = new CustomerEntity("Jeff Richter", "C") { Address = "1 Main Street", City = "Seattle" };
         await ct.ExecuteAsync(TableOperation.Insert(ce));

         // 1st client queries the entity, updates it, & saves it back
         Task tAlpha = OptimisticUpdatePatternAsync(ct, ce.PartitionKey, ce.RowKey, "Alpha");

         // 2nd client also queries the entity, updates it, & saves it back
         Task tBeta = OptimisticUpdatePatternAsync(ct, ce.PartitionKey, ce.RowKey, "Beta");
         await Task.WhenAll(tAlpha, tBeta);
         Debugger.Break();
      }

      private static Task OptimisticUpdatePatternAsync(AzureTable ct, String partitionKey, String rowKey, String newCity) {
         return AzureTable.OptimisticRetryAsync(async () => {
            // Read entity from the table (with its ETag) & modify it in memory:
            DynamicTableEntity dte = (DynamicTableEntity)
               (await ct.ExecuteAsync(TableOperation.Retrieve(partitionKey, rowKey))).Result;
            CustomerEntity ce = new CustomerEntity(dte);
            ce.City = newCity;   // Change a property's value
            MessageBox.Show(
               String.Format("NewCity={0}\r\nETag={1}\r\nHit OK to save changes", newCity, ce.ETag));
            Debugger.Break();

            // Attempt to write it back
            await ct.ExecuteAsync(TableOperation.Merge(ce));
         });
      }

      public static async Task LastWriterWins(CloudStorageAccount account) {
         CLS();
         // Preparation: Clear the table and add a customer to it
         CloudTableClient client = account.CreateCloudTableClient();
         var ct = await ((AzureTable)client.GetTableReference("Concurrency")).EnsureExistsAsync(true);
         CustomerEntity ce = new CustomerEntity("Jeff Richter", "C") { Address = "1 Main Street", City = "Seattle" };
         await ct.ExecuteAsync(TableOperation.Insert(ce));

         // Last update always wins
         CustomerEntity ce1 = new CustomerEntity((DynamicTableEntity)
            (await ct.ExecuteAsync(TableOperation.Retrieve(ce.PartitionKey, ce.RowKey))).Result);
         ce1.City = "Alpha";
         ce1.ETag = "*";   // "*" ETag matches whatever ETag the entity in the table has

         CustomerEntity ce2 = new CustomerEntity((DynamicTableEntity)
            (await ct.ExecuteAsync(TableOperation.Retrieve(ce.PartitionKey, ce.RowKey))).Result);
         ce2.City = "Beta";
         ce2.ETag = "*";   // "*" ETag matches whatever ETag the entity in the table has
         await ct.ExecuteAsync(TableOperation.Replace(ce1));  // Last update wins

         await ct.ExecuteAsync(TableOperation.Replace(ce2));  // Last update wins
      }

      public static async Task SegmentedAsync(CloudStorageAccount account) {
         CLS();
         const Int32 c_maxEntitiesPerBatch = 100;
         const String tableName = "demoSegmented";
         CloudTableClient client = account.CreateCloudTableClient();
         AzureTable ct = client.GetTableReference(tableName);

         if (await ct.CreateIfNotExistsAsync()) {
            // The table didn't exist, populate it
            TableBatchOperation tbo = null;
            for (Int32 i = 0; i < 2300; i++) {
               if ((i % c_maxEntitiesPerBatch) == 0) {
                  // Batch save all the entities 
                  if (tbo != null) await ct.ExecuteBatchAsync(tbo);
                  tbo = new TableBatchOperation();
               }
               tbo.Add(TableOperation.Insert(new CustomerEntity("PK", String.Format("Cust{0:00000}", i))));
            }
            await ct.ExecuteBatchAsync(tbo);
         }

         int iteration = 1;
         for (var tqc = ct.CreateQueryChunker(new TableQuery()); tqc.HasMore; ) {
            var chunk = await tqc.TakeAsync(200, dte => true);
            Show("{0}: {1}", iteration++, chunk.Count());
         }

         // Enumerate table entities (ExecuteQuerySegmented returns up to 1,000 items)
         for (var tqc = ct.CreateQueryChunker(new TableQuery()); tqc.HasMore; ) {
            Show("Segment results:");
            var chunk = await tqc.TakeAsync();
            foreach (var entity in chunk) Show(entity.RowKey);
         }

         // Enumerate table names (ListTablesSegmented returns up to 1,000 items)
         for (TableResultSegment rs = null; rs.HasMore(); ) {
            rs = await client.ListTablesSegmentedAsync(rs.SafeContinuationToken());
            foreach (var tablename in rs.Results) Show(tablename);
         }

         CloudTable cloudTable = null;
         TableQuery query = new TableQuery();
         for (TableQuerySegment<DynamicTableEntity> querySegment = null; querySegment == null || querySegment.ContinuationToken != null; ) {
            // Query the Table's first (or next) segment
            querySegment = await cloudTable.ExecuteQuerySegmentedAsync(query,
               querySegment == null ? null : querySegment.ContinuationToken);

            // Process the segment's entities
            foreach (DynamicTableEntity dte in querySegment.Results) {
               // TODO: Place code to process each entity
            }
         }
      }

      public static async Task EntityGroupTransactionAsync(CloudStorageAccount account) {
         CLS();
         AzureTable ct = await ((AzureTable)account.CreateCloudTableClient().GetTableReference("EGT")).EnsureExistsAsync(true);
         // Add a single entity with one PartitionKey value
         await ct.ExecuteAsync(TableOperation.Insert(
            new CustomerEntity("Paul Mehner", "C") { Address = "123 Front Street", City = "Olympia" }));

         // Add many entities sharing the same PartitionKey value
         TableBatchOperation tbo = new TableBatchOperation();
         tbo.Add(TableOperation.Insert(
            new CustomerEntity("Jeff Richter", "C") { Address = "1 Main Street", City = "Seattle" }));
         tbo.Add(TableOperation.Insert(
            new OrderEntity("Jeff Richter", "O_Milk") { Item = "Milk", Quantity = 2 }));
         tbo.Add(TableOperation.Insert(
            new OrderEntity("Jeff Richter", "O_Cereal") { Item = "Cereal", Quantity = 1 }));
         await ct.ExecuteBatchAsync(tbo);

         var query = new TableQuery {
            FilterString = new TableFilterBuilder<TableEntity>()
               .And(te => te.PartitionKey, CompareOp.EQ, "Jeff Richter")
         };

         for (var tqc = ct.CreateQueryChunker(query); tqc.HasMore; ) {
            tbo = new TableBatchOperation();
            foreach (DynamicTableEntity dte in await tqc.TakeAsync()) {
               switch ((EntityKind)(dte.Get((Int32)EntityKind.Customer, "Kind"))) { // What kind of entity is this?
                  case EntityKind.Customer:
                     var customer = new CustomerEntity(dte);
                     customer.City = "Philadelphia";
                     tbo.Add(TableOperation.Merge(customer));
                     break;
                  case EntityKind.Order:
                     var order = new OrderEntity(dte);
                     if (order.Item == "Milk") {
                        order.Quantity = 5;
                        tbo.Add(TableOperation.Merge(order));
                     }
                     break;
               }
            }
         }
         await ct.ExecuteBatchAsync(tbo);
      }

      private static async Task ShowAllCustomersAndOrdersAsync(AzureTable ct) {
         // Query all customers & each customer's orders:
         var customerQuery = new TableQuery {
            FilterString = new TableFilterBuilder<CustomerEntity>()
               .And(ce => (Int32)ce.Kind, CompareOp.EQ, (Int32)EntityKind.Customer)
         };

         for (var customerTqc = ct.CreateQueryChunker(customerQuery); customerTqc.HasMore; ) {
            foreach (var ce in await customerTqc.TakeAsync()) {
               Show("Customer: " + ce);
               var orderQuery = new TableQuery {
                  FilterString = new TableFilterBuilder<OrderEntity>()
                     .And(oe => oe.PartitionKey, CompareOp.EQ, ce.PartitionKey)
                     .And(oe => (Int32)oe.Kind, CompareOp.EQ, (Int32)EntityKind.Order)
               };
               for (var orderTqc = ct.CreateQueryChunker(orderQuery); orderTqc.HasMore; ) {
                  foreach (var oe in await orderTqc.TakeAsync()) Show("   Order: " + oe);
                  Show();
               }
            }
         }
      }

      public static async Task PropertyChanger(CloudStorageAccount account) {
         AzureTable ct = await ((AzureTable)account.CreateCloudTableClient().GetTableReference("Changer")).EnsureExistsAsync(true);
         var sas = ct.GetSharedAccessSignature(
            new SharedAccessTablePolicy {
               Permissions = SharedAccessTablePermissions.Add
               | SharedAccessTablePermissions.Query
               | SharedAccessTablePermissions.Update,
               SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddMinutes(10)
            }, null, null, null, null, null);

         ct = new CloudTable(ct.Uri, new StorageCredentials(sas));
         var dte = new DynamicTableEntity {
            Properties = {
            { "A", new EntityProperty("A") }, { "B", new EntityProperty("B") },
            { "C", new EntityProperty("C") }, { "D", new EntityProperty("D") },
            { "E", new EntityProperty("E") }, { "F", new EntityProperty(123) },
                                            }
         };
         dte.PartitionKey = "PK";
         dte.RowKey = "1"; await ct.ExecuteAsync(TableOperation.Insert(dte));
         dte.RowKey = "2"; await ct.ExecuteAsync(TableOperation.Insert(dte));
         dte.RowKey = "3"; await ct.ExecuteAsync(TableOperation.Insert(dte));

         var pc = new PropertyChanger(ct);
         await pc.RemoveAsync("B", "A"); // C, D, E, & F remain
         await pc.KeepOnlyAsync("D", new PropertyReplacer("F", "FF")); // Keep only D, and rename F->FF
      }

      public static async Task CopyTableAsync(CloudStorageAccount account) {
         AzureTable source = account.CreateCloudTableClient().GetTableReference("source");
         await source.CreateIfNotExistsAsync().ConfigureAwait(false);
         for (int e = 0; e < 250; e++) {
            await source.ExecuteAsync(TableOperation.InsertOrMerge(new DynamicTableEntity(String.Empty, e.ToString())));
         }
         AzureTable target = account.CreateCloudTableClient().GetTableReference("target");
         await target.CreateIfNotExistsAsync().ConfigureAwait(false);
         var stats = await source.CopyToTableAsync(target, TableOperationType.InsertOrMerge);
      }

      // http://blogs.msdn.com/b/windowsazurestorage/archive/2012/06/12/introducing-table-sas-shared-access-signature-queue-sas-and-update-to-blob-sas.aspx
      public static async Task SignedAccessSignatures(CloudStorageAccount account) {
         CLS();
         // Create a table
         AzureTable ct = await ((AzureTable)account.CreateCloudTableClient().GetTableReference("sasTable")).EnsureExistsAsync(true);

         // Create a SAS for the table from StartPK/StartRK to EndPK/EndRK
         SharedAccessTablePolicy policy = new SharedAccessTablePolicy {
            SharedAccessExpiryTime = DateTime.UtcNow.AddHours(1),
            Permissions = SharedAccessTablePermissions.Add
                | SharedAccessTablePermissions.Query
                | SharedAccessTablePermissions.Update
                | SharedAccessTablePermissions.Delete
         };

         String sas = ct.GetSharedAccessSignature(
             policy,    // access policy
             null,      // access policy identifier
             "PK",      // start partition key
             null,      // start row key
             "PK",      // end partition key
             null);     // end row key

         // Create a new CloudTable not using the account's key
         ct = new CloudTable(ct.Uri, new StorageCredentials(sas));

         // This should work:
         await ct.ExecuteAsync(TableOperation.Insert(new TableEntity("PK", "RK")));

         // This should fail:
         try {
            await ct.ExecuteAsync(TableOperation.Insert(new TableEntity("PK1", "RK")));
         }
         catch (StorageException ex) { // 404 (Not found)
            Show(String.Format("Failure: Status={0}({0:D}), Msg={1}",
               (HttpStatusCode)ex.RequestInformation.HttpStatusCode,
               ex.RequestInformation.HttpStatusMessage));
         }
      }

      public sealed class SchemaV1 : TableEntity {
         public SchemaV1() { }
         public SchemaV1(String pk, String rk) : base(pk, rk) { Version = 1; }
         public Int32 Version { get; set; }
         public String PropertyV1 { get; set; }
         public override String ToString() {
            return String.Format("PK={0}, RK={1}, Version={2}, PropertyV1={3}",
               PartitionKey, RowKey, Version, PropertyV1);
         }
      }

      public sealed class SchemaV2 : TableEntity {
         public SchemaV2(String pk, String rk) : base(pk, rk) { Version = 2; }
         public SchemaV2() { }
         public Int32 Version { get; set; }
         public String PropertyV1 { get; set; }
         public String PropertyV2 { get; set; }
         public override String ToString() {
            return String.Format("PK={0}, RK={1}, Version={2}, PropertyV1={3}, PropertyV2={4}",
               PartitionKey, RowKey, Version, PropertyV1, PropertyV2);
         }
      }

      public static void SchemaVersioning(CloudStorageAccount account) {
         CLS();
         CloudTable ct = ((AzureTable)account.CreateCloudTableClient().GetTableReference("versionTable")).EnsureExistsAsync(true).Result;

         // Add some entities with V1 schema to a table
         ct.Execute(TableOperation.Insert(new SchemaV1("PK", "RK1") { PropertyV1 = "V1" }));
         ct.Execute(TableOperation.Insert(new SchemaV1("PK", "RK2") { PropertyV1 = "V1" }));

         // App-V2 reads SchemaV1 and converts to SchemaV2 (added property)
         // Missing properties (eg: PropertyV2) will have default values
         IEnumerable<SchemaV2> entitiesV2 = ct.ExecuteQuery(new TableQuery<SchemaV2> { FilterString = String.Empty });

         // Convert from SchemaV1 to SchemaV2
         foreach (var e in entitiesV2) {
            e.Version = 2;
            e.PropertyV2 = "V2";
            // Don't use [InsertOr]Replace: it will delete properties that App-V1 still needs
            ct.Execute(TableOperation.InsertOrMerge(e));
         }

         // Have App-V1 read Schema-V2 entities
         IEnumerable<SchemaV1> entitiesV1 = ct.ExecuteQuery(new TableQuery<SchemaV1> { FilterString = String.Empty });
         foreach (var e in entitiesV1) {
            // App-V1 can still modify properties it knows about
            e.PropertyV1 = "Updated property";
            ct.Execute(TableOperation.InsertOrReplace(e));
            Show(e);
         }
      }

      #region Table Entity Definitions: Application-specific types
      private enum EntityKind {
         // NOTE: Explicitly assign values to these symbols and never change them!
         Customer = 0,
         Order = 1
      }

      private abstract class KindEntity : EntityBase {
         protected KindEntity(DynamicTableEntity dte = null) : base(dte) { }

         public override String ToString() { return String.Format("Kind={0}", Kind); }

         // Consider adding version property to assist with schema changes}
         public EntityKind Kind {
            get { return (EntityKind)Get((Int32)EntityKind.Order); }
            protected set { Set((Int32)value); }
         }
      }

      private sealed class CustomerEntity : KindEntity {
         public CustomerEntity(DynamicTableEntity dte) : base(dte) { }
         public CustomerEntity(String partitionKey, String rowKey)
            : base() {
            PartitionKey = partitionKey;
            RowKey = rowKey;
            Kind = EntityKind.Customer;
         }
         public String Address {
            get { return Get(String.Empty); }
            set { Set(value); }
         }
         public String City {
            get { return Get(String.Empty); }
            set { Set(value); }
         }
         public override String ToString() {
            return String.Format("{0}, Address={1}, City={2}", base.ToString(), Address, City);
         }
      }

      private sealed class OrderEntity : KindEntity {
         public OrderEntity(DynamicTableEntity dte) : base(dte) { }
         public OrderEntity(String partitionKey, String rowKey)
            : base() {
            PartitionKey = partitionKey;
            RowKey = rowKey;
            Kind = EntityKind.Order;
         }
         public String Item {
            get { return Get(String.Empty); }
            set { Set(value); }
         }
         public Int32 Quantity {
            get { return Get(0); }
            set { Set(value); }
         }

         public override String ToString() {
            return String.Format("{0}, Item={1}, Quantity={2}", base.ToString(), Item, Quantity);
         }
      }
      #endregion
   }

   private static class QueuePatterns {
      public static async Task Basics(CloudStorageAccount account) {
         CLS();
         CloudQueueClient client = account.CreateCloudQueueClient();
         AzureQueue queue = await ((AzureQueue)client.GetQueueReference("demo")).EnsureExistsAsync(true);

         // Producers add messages to a queue:
         await queue.AddMessageAsync(new CloudQueueMessage("MsgA"));
         await queue.AddMessageAsync(new CloudQueueMessage("MsgB"));

         await queue.FetchAttributesAsync();   // Call this 1st to get ApproximateMesageCount
         Int32? msgCount = queue.ApproximateMessageCount;

         // Consumers process messages:
         var visibilityTimeout = TimeSpan.FromSeconds(5); // Max=7 days
         while (true) {
            const Int32 c_MaxMsgsToGet = 32;
            IEnumerable<CloudQueueMessage> msgs =
               await queue.GetMessagesAsync(c_MaxMsgsToGet, visibilityTimeout, null, null);
            if (msgs.Count() == 0) {
               Thread.Sleep(1000);  // IMPORTANT: Balance cost with latency (once a second for 1 month=$0.27)
               continue;
            }
            foreach (CloudQueueMessage cqm in msgs) {
               Show(QueueMessageToString(cqm)); // For demo
               if (cqm.DequeueCount < 3) {
                  // Process message here...
               } else {
                  // Log poison msg to fix code here...
               }
               await queue.DeleteMessageAsync(cqm.Id, cqm.PopReceipt);
            }
            Show();
         }
         // NOTE: This method never returns as is typical for this kind of processing
      }

      private static String QueueMessageToString(CloudQueueMessage cqm) {
         var sb = new StringBuilder();
         sb.AppendLine("Id:              " + cqm.Id);
         sb.AppendLine("InsertionTime:   " + cqm.InsertionTime);
         sb.AppendLine("ExpirationTime:  " + cqm.ExpirationTime);
         sb.AppendLine("DequeueCount:    " + cqm.DequeueCount);
         sb.AppendLine("Message text:    " + cqm.AsString);
         sb.AppendLine("NextVisibleTime: " + cqm.NextVisibleTime);
         sb.AppendLine("PopReceipt:      " + cqm.PopReceipt);
         return sb.ToString();
      }

      public struct IdempotentMessage {
         // Identify the table entity to operate on:
         public readonly String PartitionKey;
         public readonly String RowKey;

         // Add other "arguemnts" here:
         public readonly String Operation;
         public readonly Int32 Operand;
         public IdempotentMessage(String pk, String rk, String operation, Int32 operand) {
            PartitionKey = pk;
            RowKey = rk;
            Operation = operation;
            Operand = operand;
         }
         public override string ToString() {
            return String.Format("{0},{1},{2},{3}", PartitionKey, RowKey, Operation, Operand);
         }
         public static IdempotentMessage Parse(String msg) {
            String[] tokens = msg.Split(',');
            return new IdempotentMessage(tokens[0], tokens[1], tokens[2], Int32.Parse(tokens[3]));
         }
      }

      public static class Idempotency {
         public static async Task Demo(CloudStorageAccount account) {
            CLS();
            // Initialize the producer and the consumer:
            AzureTable table = await ((AzureTable)account.CreateCloudTableClient().GetTableReference("Idempotent")).EnsureExistsAsync(true);
            AzureQueue queue = await ((AzureQueue)account.CreateCloudQueueClient().GetQueueReference("idempotent")).EnsureExistsAsync(true);

            // Create an entity we can modify
            String pk = "PK", rk = "RK";
            await table.ExecuteAsync(TableOperation.Insert(new DynamicTableEntity(pk, rk) {
               Properties = { { "Value", new EntityProperty(0) } }
            }));

            // Producer creates an idempotent message & queues it:
            var iMsg = new IdempotentMessage(pk, rk, "+", 5);
            await queue.AddMessageAsync(new CloudQueueMessage(iMsg.ToString()));

            // Consumer gets the idempotent msg from queue & processes it
            CloudQueueMessage qMsg = await queue.GetMessageAsync();
            iMsg = IdempotentMessage.Parse(qMsg.AsString);

            Boolean weDidIt = await ProcessIdempotentMessageAsync(table, qMsg.Id, iMsg, "ProcessedQueueMessages");
            // weDidIt is true if this call did it; false if something else did it

            // ***************** Cleanup msg ID if we know if could never be processed again ************************************
            // If the batch succeeds (or if the MSGID is already processed), then we can delete the msg from the queue
            Boolean msgDeleted = await queue.DeleteMessageAsync(qMsg);
            // TODO: If msg is removed from queue without an exception & dequeue count is 1, then this popper is the owner and we know
            // that no other PC can get the msg; so, we can delete this msg ID from the entity (this should be the common case)

            // Demos the MsgIdCollector
            await ProcessedMsgCollectorAsync(table, DateTimeOffset.UtcNow/*.AddDays(-1)*/);
         }

         // This version puts the MsgIDs as a property in the entity itself
         public static Task<Boolean> ProcessIdempotentMessageAsync(AzureTable table, String msgId,
            IdempotentMessage iMsg, String processedQueueMessagesProperty) {
            Guid qMsgId = Guid.Parse(msgId);
            // The Msg's Id is the operation's Id. We the ID to the entity's processedQueueMessages property 
            // indicating that this msg has been processed.

            return AzureTable.OptimisticRetryAsync(async () => {
               // Read the entity we want to change to get its current state & ETag
               var entity = (DynamicTableEntity)
                  (await table.ExecuteAsync(TableOperation.Retrieve(iMsg.PartitionKey, iMsg.RowKey))).Result;

               // Get the set of MsgIDs that have been processed on this entity
               EntityProperty processedQueueMsgIds = null;
               if (!entity.Properties.TryGetValue(processedQueueMessagesProperty, out processedQueueMsgIds)) {
                  processedQueueMsgIds = new EntityProperty((Byte[])null);
                  entity.Properties.Add(processedQueueMessagesProperty, processedQueueMsgIds);
               }

               // If this MsgId has already been processed; nothing to do 
               if (IsGuidInList(qMsgId, processedQueueMsgIds.BinaryValue)) return false;

               // If this MsgId hasn't been processed, add it to the collection (discarding old MsgIds)
               processedQueueMsgIds.BinaryValue = AppendGuidToList(qMsgId, processedQueueMsgIds.BinaryValue);

               // Perform the message's operation
               switch (iMsg.Operation) {
                  case "+": entity["Value"].Int32Value += iMsg.Operand; break;
                  // TODO: Insert other operations here...
               }

#if SimulateBehindTheBackUpdate
               // The code below tests what happens if the MSGID entity exists
               EntityProperty msgIdPropertyTest = null;
               entity.Properties.TryGetValue("MsgIds", out msgIdPropertyTest);
               List<Guid> msgIdsTest = GuidBytesToList((msgIdPropertyTest == null) ? null : msgIdPropertyTest.BinaryValue);
               msgIdsTest.Add(msgId);
               entity["MsgIds"] = new EntityProperty(ListToGuidBytes(msgIdsTest));
               m_table.Execute(TableOperation.Merge(entity));
#endif

               // Update the entity recording that this msg was processed
               await table.ExecuteAsync(TableOperation.Merge(entity));
               return true;
            });
         }

         // This version creates a new entity for each MsgId in the same table & PartitionKey
         public static Task<Boolean> ProcessIdempotentMessageAsync2(AzureTable table, String msgId,
            IdempotentMessage iMsg, String rowKeyPrefix) {
            Guid qMsgId = Guid.Parse(msgId);

            // The Msg's Id is the operation's Id. We add an entity to the table indicating if this msg has been processed.
            // The entity has the same PK as the entity being modified so we can perform an EGT with it.
            // The entity has a special RK:
            String rowKeyMsgId = String.Format("_MsgId__{0}__{1}", iMsg.RowKey, msgId);
            return AzureTable.OptimisticRetryAsync(async () => {
               // Look in the table to see if this msg has already been processed; nothing to do if it has
               TableResult tr = await table.ExecuteAsync(TableOperation.Retrieve(iMsg.PartitionKey, rowKeyMsgId));
               if (tr.Result != null) return false;

               // Msg not processed yet; read the entity we want to change to get its current state & ETag
               var entity = (DynamicTableEntity)
                  (await table.ExecuteAsync(TableOperation.Retrieve(iMsg.PartitionKey, iMsg.RowKey))).Result;

               // Update the entity's property to what we want it to be
               switch (iMsg.Operation) {
                  case "+": entity["Value"].Int32Value += iMsg.Operand; break;
                  // TODO: Insert other operations here...
               }

#if SimulateChangeBehindOurBack
               // The code below tests what happens if the MSGID entity exists
               await table.ExecuteAsync(TableOperation.Insert(new DynamicTableEntity(iMsg.PartitionKey, rowKeyMsgId) {
                  Properties = { { "MsgTxn", new EntityProperty(true) }, { "MsgId", new EntityProperty(msgId) } }
               }));
#endif
               // As a transaction, update the entity and insert the Msg ID row into the table
               try {
                  var tbo = new TableBatchOperation {
                     TableOperation.Merge(entity),
                     TableOperation.Insert(new DynamicTableEntity(iMsg.PartitionKey, rowKeyMsgId) {
                        Properties = {
                           { "MsgTxn", new EntityProperty(true) },
                           { "MsgId", new EntityProperty(msgId) } }
                     }) };
                  await table.ExecuteBatchAsync(tbo);
                  return true;
               }
               catch (StorageException ex) {
                  // If inserting the MSGID entity fails, then this msg was processed behind our back; we can stop
                  if (ex.Matches(HttpStatusCode.Conflict)) return false;
                  throw;   // Entity changed behind our back (retry) or unexpected exception
               }
            });
         }

         private const Int32 c_guidLength = 16; // A guid is 16 bytes
         private static Boolean IsGuidInList(Guid guid, Byte[] guidSet) {
            if (guidSet == null) return false;
            for (Int32 offset = 0; offset < guidSet.Length; offset += c_guidLength) {
               Byte[] guidBytes = new Byte[c_guidLength];
               Array.Copy(guidSet, offset, guidBytes, 0, c_guidLength);
               var v = new Guid(guidBytes);
               if (v == guid) return true;
            }
            return false;
         }

         private static Byte[] AppendGuidToList(Guid guid, Byte[] guidSet) {
            Byte[] guidBytes = guid.ToByteArray();
            if (guidSet == null) return guidBytes;
            if (guidSet.Length == 64 * 1024) {   // Max=4096
               // The byte[] is maxed out; remove the first item and append the new guid
               Array.Copy(guidSet, c_guidLength, guidSet, 0, 64 * 1024 - c_guidLength);
               Array.Copy(guidBytes, 0, guidSet, 64 * 1024 - c_guidLength, c_guidLength);
            } else {
               Array.Resize(ref guidSet, guidSet.Length + c_guidLength);
               Array.Copy(guidBytes, 0, guidSet, guidSet.Length - c_guidLength, c_guidLength);
            }
            return guidSet;
         }

         // When you're sure a MSG can't be processed by other machines, you can delete its MSGID entity
         // One option is to have a collector that scans the whole table for entities whose MSGID property exists 
         // and whose Timestamp is old [Example: Timestamp < (today - 1 day)]
         public static async Task ProcessedMsgCollectorAsync(AzureTable table, DateTimeOffset deleteMsgsProcessedBefore) {
            TableQuery tq = new TableQuery {
               FilterString = new TableFilterBuilder<TableEntity>()
               .And(te => te.Timestamp, CompareOp.LT, deleteMsgsProcessedBefore) + " and MsgTxn"
            };
            for (var tqc = table.CreateQueryChunker(tq); tqc.HasMore; ) {
               foreach (var dte in await tqc.TakeAsync())
                  await table.ExecuteAsync(TableOperation.Delete(dte));
            }
         }
      }

      public static void Segmented(CloudStorageAccount account) {
         CloudQueueClient client = account.CreateCloudQueueClient();
         for (QueueResultSegment qrs = null; qrs.HasMore(); ) {
            qrs = client.ListQueuesSegmented(qrs.SafeContinuationToken());
            foreach (AzureQueue q in qrs.Results)
               Show(q.Uri);
         }
      }
   }

   static AzureStoragePatterns() {
      String[] args = Environment.GetCommandLineArgs();
      if (args.Length == 1) { // This instance run by a user, spawn the storage killer
         while (!Process.GetProcessesByName("WAStorageEmulator").Any()) {
            try {
               var psi = new ProcessStartInfo(@"C:\Program Files (x86)\Microsoft SDKs\Azure\Storage Emulator\WAStorageEmulator.exe", "start") { WindowStyle = ProcessWindowStyle.Hidden };
               Process.Start(psi);
               break;   // Started ok, break out
            }
            catch (System.ComponentModel.Win32Exception ex) {
               var e = ex;
               MessageBox.Show("Start the Windows Azure Storage Emulator");
            }
         }

         if (c_SpawnStorageKiller) {
            var psi = new ProcessStartInfo(args[0], Process.GetCurrentProcess().Id.ToString()) { WindowStyle = ProcessWindowStyle.Minimized };
            Process.Start(psi);
         }
      } else { // This instance is the storage killer
         Process p = Process.GetProcessById(Int32.Parse(args[1]));
         Console.BackgroundColor = ConsoleColor.DarkRed;
         Console.Clear();
         Console.Title = "WADS demo storage killer (waiting for demo app to terminate)";
         p.WaitForExit();
         Console.WriteLine("Press Ctrl-C to keep storage or any other key to delete now.");
         var stop = DateTime.Now + TimeSpan.FromSeconds(30);
         while (DateTime.Now < stop && !Console.KeyAvailable) {
            Console.Title = String.Format("Deleting storage in {0} second(s).",
               (Int32)((stop - DateTime.Now).TotalSeconds));
            Thread.Sleep(400);
         }

         DeleteDemoStorage(GetStorageAccount(StorageAccountType.DevStorage));
         DeleteDemoStorage(GetStorageAccount(StorageAccountType.AzureStorage));
         Show(); Show("Storage deleted");
         Thread.Sleep(5000); // Let the user see the deleted storage objects
         Environment.Exit(0);
      }
   }

   private static void DeleteDemoStorage(CloudStorageAccount account) {
      // Blobs
      CloudBlobClient blobClient = account.CreateCloudBlobClient();
      CloudBlobContainer rootContainer = blobClient.GetContainerReference("$root");
      try {
         rootContainer.Delete();
         Show("Deleting blob container: " + rootContainer.Uri);
      }
      catch (StorageException) { }
      foreach (var c in blobClient.ListContainers().Where(container => container.Name.StartsWith("demo"))) {
         Show("Deleting blob container: " + c.Uri);
         c.Delete();
      }

      // Tables
      CloudTableClient tableClient = account.CreateCloudTableClient();
      foreach (var table in tableClient.ListTables().Where(ct => ct.Name.StartsWith("demo"))) {
         Show("Deleting table: " + table.Name);
         table.Delete();
      }

      // Queues
      CloudQueueClient queueClient = account.CreateCloudQueueClient();
      foreach (var queue in queueClient.ListQueues().Where(q => q.Name.StartsWith("demo"))) {
         Show("Deleting queue: " + queue.Uri);
         queue.Delete();
      }
   }

   #region Show Methods
   [DebuggerStepThrough]
   private static void CLS() { Console.Clear(); }
   private struct NewLines {
      [DebuggerStepThrough]
      public void NL(Int32 newLines = 1) {
         for (; newLines > 0; newLines--) Console.WriteLine();
      }
   }

   [DebuggerStepThrough]
   private static NewLines Show(Object arg1 = null) {
      Console.WriteLine(arg1); return new NewLines();
   }

   [DebuggerStepThrough]
   private static NewLines Show(String format, params Object[] args) {
      Console.WriteLine(format, args);
      return new NewLines();
   }

   [DebuggerStepThrough]
   private static void ShowIE(Object uri) {
      Process.Start("IExplore", uri.ToString()).WaitForExit();
   }
   #endregion
}

#if FACEBOOK
internal static class Facebook {
   /*
    * User table: UserId(RK), name(PK), & other info
    * Friend table: UserId (user,PK) -> UserId (friends, RK) [repeat for each friend]
    * Status table: UserId(PK) -> text (RK)
    */
   private sealed class User : TableServiceEntity {
      public User(String id, String name) : base(name, id) { Id = id; Name = name; }
      public String Id { get; set; }
      public String Name { get; set; }
   }
   private sealed class Friend : TableServiceEntity {
      public Friend(String userId, String friendId) : base(userId, friendId) {
         UserId = userId;
         FriendId = friendId;
      }
      public String UserId { get; set; }
      public String FriendId { get; set; }
   }
   private sealed class Status : TableServiceEntity {
      public Status(String userId, String text)
         : base(userId, DateTime.UtcNow.ToString()/* TODO: reverse*/) {
            UserId = userId;
            When = DateTime.UtcNow;
            StatusText = text;
      }
      public String UserId { get; set; }
      public DateTime When { get; set; }
      public String StatusText { get; set; }
   }
}
#endif

#if CustomerEntitySlide
internal sealed class CustomerEntitySlide {
   public readonly DynamicTableEntity TableEntity;
   public CustomerEntitySlide(DynamicTableEntity dte = null) { TableEntity = dte ?? new DynamicTableEntity(); }
   public CustomerEntitySlide(String partitionKey, String rowKey)
      : this() {
      TableEntity.PartitionKey = partitionKey;
      TableEntity.RowKey = rowKey;
   }
   public String ETag {
      get { return TableEntity.ETag; }
      set { TableEntity.ETag = value; }
   }
   public String PartitionKey {
      get { return TableEntity.PartitionKey; }
      set { TableEntity.PartitionKey = value; }
   }
   public String RowKey {
      get { return TableEntity.RowKey; }
      set { TableEntity.RowKey = value; }
   }
   public DateTimeOffset Timestamp { get { return TableEntity.Timestamp; } }
   public String Name {
      get { return Get<String>(null).StringValue; }
      set { Set(new EntityProperty(value)); }
   }
   public String City {
      get { return Get<String>(null).StringValue; }
      set { Set(new EntityProperty(value)); }
   }
   public override String ToString() {
      return String.Format("{0}, Name={1}, City={2}", base.ToString(), Name, City);
   }

   private EntityProperty Get<T>(T defaultValue, [CallerMemberName] String tablePropertyName = null) {
      return TableEntity.Get<T>(defaultValue, tablePropertyName);
   }

   private void Set(EntityProperty value, [CallerMemberName] String tablePropertyName = null) {
      TableEntity.Set(value, tablePropertyName);
   }
}
#endif

public enum Level { Dungeon, Castle, Forrest, /* ... */ }
public sealed class CompletedLevel {
   public readonly Level Level;
   public readonly DateTimeOffset Completed;
   public CompletedLevel(Level level, DateTimeOffset completed) {
      Level = level; Completed = completed;
   }

   #region Serialization & Deserialization members
   private const UInt16 c_version = 0; // Increment this when you change (de)serialization format 
   private const Int32 c_ApproxRecordSizeV0 = sizeof(Level) + sizeof(Int64);
   internal static Byte[] Serialize(IEnumerable<CompletedLevel> completedLevels) {
      // Serialize the collection to a byte array
      return PropertySerializer.ToProperty(
         maxRecordsPerEntity: AzureTable.MaxPropertyBytes / c_ApproxRecordSizeV0,
         version: c_version,
         records: completedLevels,
         approxRecordSize: c_ApproxRecordSizeV0,   // Improves performance
         recordToProperty: (chapter, version, bytes) => chapter.Serialize(version, bytes));
   }
   private void Serialize(UInt16 version, PropertySerializer propertyBytes) {
      switch (version) {
         case 0:
            propertyBytes.Add((Int32)Level).Add(Completed.UtcTicks);
            break;
         default:
            throw new InvalidOperationException("'version' identifies an unknown format.");
      }
   }
   internal static IEnumerable<CompletedLevel> Deserialize(Byte[] bytes) {
      // Deserialize the byte array to a collection
      return PropertyDeserializer.FromProperty(bytes,
         (version, deserializer) => new CompletedLevel(version, deserializer));
   }
   private CompletedLevel(UInt16 version, PropertyDeserializer propertyBytes) {
      switch (version) {
         case 0:
            Level = (Level)propertyBytes.ToInt32();
            Completed = new DateTimeOffset(propertyBytes.ToInt64(), TimeSpan.Zero);
            break;
         default:
            throw new InvalidOperationException("'version' identifies an unknown format.");
      }
   }
   #endregion
}

public sealed class GamerEntity : TableEntityBase {
   /// <summary>
   /// Factory method that looks up a Gamer via their gamer tag.
   /// </summary>
   /// <param name="table">Table to look up gamer in.</param>
   /// <param name="tag">Gamer tag identifying the gamer.</param>
   /// <returns>The GamerEntity (if exists) or throws EntityNotFoundException.</returns>
   public static async Task<GamerEntity> FindAsync(AzureTable table, String tag) {
      // Try to find gamer via their gamer tag
      // NOTE: Throws EntityNotFoundException if tag not found
      DynamicTableEntity dte = await EntityBase.FindAsync(table, tag.ToLowerInvariant(), String.Empty);

      // Wrap DynamicTableEntity object with .NET class object & return wrapper
      return new GamerEntity(table, dte);
   }

   /// <summary>
   /// Wraps a GamerEntity object around a DynamicTableEntity and an Azure table.
   /// </summary>
   /// <param name="table">The table the GamerEntity should interact with.</param>
   /// <param name="dte">The DynamicTableEntity object this .NET class object wraps.</param>
   public GamerEntity(AzureTable table, DynamicTableEntity dte = null) : base(table, dte) { }

   /// <summary>
   /// Creates a new GamerEntity object around a new DynamicTableEntity and Azure table.
   /// </summary>
   /// <param name="table">The table the GamerEntity should interact with.</param>
   /// <param name="tag">The gamer tag uniquely identifying the gamer.</param>
   public GamerEntity(AzureTable table, String tag)
      : this(table) {
      // Gamer is identified with PK=Tag (lowercase) & RowKey=""
      PartitionKey = tag.ToLowerInvariant();
      RowKey = String.Empty;  // This example doesn't use the RowKey for anything

      Tag = tag;    // Save the case-preserved gamer tag value
   }

   /// <summary>
   /// The gamer's tag (case-preserved).
   /// </summary>
   // This property's .NET name is different from its entity name 
   public String Tag {  // .NET property name
      get { return Get(String.Empty, "GamerTag"); } // Entity property name
      private set { Set(value, "GamerTag"); }       // Entity property name
   }

   /// <summary>
   /// The gamer's current score (defaults to 0).
   /// </summary>
   // This property's .NET name is the same as its entity name 
   public Int32 Score {
      get { return Get(0); } // If "Score" property doesn't exist, return 0
      set { Set(value); }
   }

   // VERSIONING: You can easily add/remove .NET properties in the future.
   // Old entities without the property will return the default value.
   // If you remove a .NET property, the entity property will just be ignored.
   // If you remove a .NET property and Replace the entity, the property 
   // will be removed from the entity.

   /// <summary>
   /// A string showing the gamer's info (for debugging).
   /// </summary>
   /// <returns>Gamer info.</returns>
   public override String ToString() {
      return String.Format("Tag={0}, Score={1}", Tag, Score);
   }

   #region Members supporting the Gamer's CompletedLevel collection
   private static readonly Byte[] s_empty = new Byte[0];
   private const Int32 c_maxCompletedLevelProperties = 1;
   public const String CompletedLevelsPropertyName = "CompletedLevels";

   // I prefer methods here instead of a property because these methods are
   // computationally expensive and I want callers to carefully consider
   // calling them. Callers tend to access properties without much thought
   // as to what they do internally.
   /// <summary>
   /// Returns completed levels associated with the Gamer.
   /// </summary>
   public IEnumerable<CompletedLevel> GetCompletedLevels() {
      // Read the byte array containing the serialized CompletedLevel collection
      Byte[] bytes = Get(s_empty, c_maxCompletedLevelProperties, CompletedLevelsPropertyName);

      // Deserialize the byte array to a collection
      return CompletedLevel.Deserialize(bytes);
   }

   /// <summary>
   /// Associates completed levels with the Gamer.
   /// </summary>
   /// <param name="completedLevels"></param>
   /// <returns></returns>
   public GamerEntity SetCompletedLevels(IEnumerable<CompletedLevel> completedLevels) {
      // Serialize the Chapter collection to a byte array
      Byte[] bytes = CompletedLevel.Serialize(completedLevels);

      // Save the byte array to a single table property
      Set(bytes, c_maxCompletedLevelProperties, CompletedLevelsPropertyName);
      return this;
   }
   #endregion
}

public sealed class GamerManager {
   public static async Task DemoAsync() {
      GamerManager gm = new GamerManager();

      // Create 2 gamers
      GamerEntity dragon = await gm.CreateGamerAsync("Dragon");
      GamerEntity creeper = await gm.CreateGamerAsync("Creeper");

      // Increase a Gamer's score & indicate what level they completed
      await gm.IncreaseScoreAsync("Dragon", 10);
      await gm.SetCompletedLevelAsync("Dragon", Level.Castle, DateTimeOffset.UtcNow);

      await gm.ShowGamersAsync(20, 40, true); // Shows 0 gamers

      // Increase a Gamer's score & indicate what level they completed
      await gm.IncreaseScoreAsync("Creeper", 20);
      await gm.SetCompletedLevelAsync("Creeper", Level.Forrest, DateTimeOffset.UtcNow);

      await gm.ShowGamersAsync(20, 40, false); // Shows 1 gamer

      // Increase a Gamer's score & indicate what level they completed
      await gm.IncreaseScoreAsync("Dragon", 20);
      await gm.SetCompletedLevelAsync("Dragon", Level.Forrest, DateTimeOffset.UtcNow);

      await gm.ShowGamersAsync(20, 40, true); // Shows 2 gamers
   }

   private readonly AzureTable m_table = Initialize(true);

   /// <summary>
   /// Creates (& optionally clears) an Azure table that holds all the Gamers.
   /// </summary>
   /// <param name="clear">True to clear the table.</param>
   private static AzureTable Initialize(Boolean clear = false) {
      AzureTable table = CloudStorageAccount.DevelopmentStorageAccount
         .CreateCloudTableClient().GetTableReference("Gamers");
      if (clear) table.DeleteIfExistsAsync().GetAwaiter().GetResult();
      table.CreateIfNotExistsAsync().GetAwaiter().GetResult();
      return table;
   }

   /// <summary>
   /// Inserts a new Gamer (via their tag) into the Azure table.
   /// </summary>
   /// <param name="tag">The gamer tag uniquely identifying the gamer.</param>
   /// <returns>The GamerEntity just inserted into the table.</returns>
   /// <exception cref="StorageException">Thrown if tag already in use.</exception>
   public async Task<GamerEntity> CreateGamerAsync(String tag) {
      GamerEntity gamer = new GamerEntity(m_table, tag);
      // NOTE: Throws StorageException (HttpStatusCode=409 [Conflict]) if tag already in use
      await gamer.InsertAsync();
      return gamer;
   }

   /// <summary>
   /// Increases a gamer's score.
   /// </summary>
   /// <param name="tag">The gamer tag uniquely identifying the gamer.</param>
   /// <param name="increase">The amount to increase gamer's score by (can be negative).</param>
   /// <returns>The gamer's score after the change.</returns>
   public Task<Int32> IncreaseScoreAsync(String tag, Int32 increase) {
      return AzureTable.OptimisticRetryAsync(async () => {
         GamerEntity gamer = await GamerEntity.FindAsync(m_table, tag);
         gamer.Score += increase;
         await gamer.MergeAsync();
         return gamer.Score;  // Returns the gamer's new score
      });
   }

   /// <summary>
   /// Record's that gamer completed a game level.
   /// </summary>
   /// <param name="tag">The gamer tag uniquely identifying the gamer.</param>
   /// <param name="level">The level the gamer just completed.</param>
   /// <param name="completed">The time when the gamer completed the level.</param>
   public Task SetCompletedLevelAsync(String tag, Level level, DateTimeOffset completed) {
      return AzureTable.OptimisticRetryAsync(async () => {
         // Find the Gamer
         GamerEntity gamer = await GamerEntity.FindAsync(m_table, tag);

         // Get Gamer's CompletedLevels, append new CompletedLevel, set CompletedLevels
         IEnumerable<CompletedLevel> levels = gamer.GetCompletedLevels();
         if (levels.Any(l => l.Level == level)) return; // Level already completed; nothing to do

         levels = levels.Concat(new[] { new CompletedLevel(level, completed) });
         gamer.SetCompletedLevels(levels);
         await gamer.MergeAsync();  // Update the entity in the table
      });
   }

   /// <summary>
   /// Displays all gamers with a score between two values.
   /// </summary>
   /// <param name="lowScore">The low score.</param>
   /// <param name="highScore">THe high score.</param>
   public async Task ShowGamersAsync(Int32 lowScore, Int32 highScore, Boolean showCompletedLevels) {

      Console.WriteLine("GAMERS WITH SCORES BETWEEN {0} AND {1}:", lowScore, highScore);

      // Create a query that scans the whole table looking for gamers 
      // with a score between lowScore & highScore inclusive
      TableQuery query = new TableQuery {
         FilterString = new TableFilterBuilder<GamerEntity>()
            .And(ge => ge.Score, CompareOp.GE, lowScore)
            .And(ge => ge.Score, CompareOp.LE, highScore),

         // This shows to request a subset of properties (Tag & Score) be returned
         SelectColumns = new PropertyName<GamerEntity>()[ge => ge.Tag, ge => ge.Score].ToList()
      };
      // This conditionally adds another property to be returned
      if (showCompletedLevels) query.SelectColumns.Add(GamerEntity.CompletedLevelsPropertyName);

      // Prepare to execute the query in chunks (segments)
      for (TableQueryChunk chunker = m_table.CreateQueryChunker(query); chunker.HasMore; ) {

         // Query a result segment
         foreach (DynamicTableEntity dte in await chunker.TakeAsync()) {

            // Wrap the DynamicTableEntity in a type-safety, IntelliSense, Refactoring class (BookEntity)
            GamerEntity gamer = new GamerEntity(m_table, dte);

            // Now, manipulate the entity
            Console.WriteLine("Tag={0}, Score={1}", gamer.Tag, gamer.Score);
            foreach (CompletedLevel completedLevel in gamer.GetCompletedLevels()) {
               Console.WriteLine("   {0,-10}   {1}", completedLevel.Level, completedLevel.Completed);
            }
            Console.WriteLine();

            // We could modify the entity here and update the table by calling be.MergeAsync
            // Or, we could delete the entity by calling be.DeleteAsync

            // Or, we could add an entity operation to a TableBatchOperation by calling 
            // Insert, InsertOrMerge, InsertOrReplace, Merge, Replace, or Delete.
         }
      }
      Console.WriteLine();
   }
}