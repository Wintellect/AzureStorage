using System;
using System.Globalization;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Text;
using Windows.Security.Cryptography;
using Windows.Security.Cryptography.Core;
using Windows.Storage.Streams;

namespace Wintellect.Azure.Storage.Sas {
   // Service versioning: http://msdn.microsoft.com/en-us/library/windowsazure/dd894041.aspx
   // Constructing the Shared Access Signature URI: http://msdn.microsoft.com/en-us/library/windowsazure/dn140255.aspx
   // Permissions order: Container=rwdl, Blob=rwd, Queue=raup, Table=raud
   /// <summary>The set of blob container permissions.</summary>
   [Flags]
   public enum SasContainerPermissions {
      /// <summary>No permission.</summary>
      None = 0,
      /// <summary>Read permission.</summary>
      Read = 'r',
      /// <summary>Write permission.</summary>
      Write = 'w' << 8,
      /// <summary>Delete permission.</summary>
      Delete = 'd' << 16,
      /// <summary>List permission.</summary>
      List = 'l' << 24
   }
   /// <summary>The set of blob permissions.</summary>
   [Flags]
   public enum SasBlobPermissions {
      /// <summary>No permission.</summary>
      None = 0,
      /// <summary>Read permission.</summary>
      Read = 'r',
      /// <summary>Write permission.</summary>
      Write = 'w' << 8,
      /// <summary>Delete permission.</summary>
      Delete = 'd' << 16 
   }
   /// <summary>The set of queue permissions.</summary>
   [Flags]
   public enum SasQueuePermissions {
      /// <summary>No permission.</summary>
      None = 0,
      /// <summary>Read permission.</summary>
      Read = 'r',
      /// <summary>Add permission.</summary>
      Add = 'a' << 8,
      /// <summary>Update permission.</summary>
      Update = 'u' << 16,
      /// <summary>Process messages permission.</summary>
      ProcessMessages = 'p' << 24 
   }
   /// <summary>The set of table permissions.</summary>
   [Flags]
   public enum SasTablePermissions {
      /// <summary>No permission.</summary>
      None = 0,
      /// <summary>Query permission.</summary>
      Query = 'r',
      /// <summary>Add permission.</summary>
      Add = 'a' << 8,
      /// <summary>Update permission.</summary>
      Update = 'u' << 16,
      /// <summary>Delete permission.</summary>
      Delete = 'd' << 24
   }

   /// <summary>Defines methods for manipulating Azure storage SAS strings.</summary>
   public static class AzureStorageSas {
      /// <summary>Parses a SAS URL returning its start and end times.</summary>
      /// <param name="sasUrl">The SAS URL to parse.</param>
      /// <param name="startTime">The start time in the SAS URL.</param>
      /// <param name="endTime">The end time in the SAS URL.</param>
      public static void ParseSasUrlTimes(String sasUrl, out DateTimeOffset startTime, out DateTimeOffset endTime) {
         startTime = GetTimeAfterPrefix("st=", sasUrl) ?? DateTimeOffset.UtcNow;
         endTime = GetTimeAfterPrefix("se=", sasUrl) ?? startTime.AddHours(1);
      }

      private static DateTimeOffset? GetTimeAfterPrefix(String prefix, String url) {
         Int32 startIndex = url.IndexOf(prefix);
         if (startIndex == -1) return null;
         Int32 endIndex = url.IndexOf("&", startIndex += prefix.Length);
         if (endIndex == -1) return null;
         String time = url.Substring(startIndex, endIndex - startIndex).Replace("%3A", ":");
         DateTimeOffset dt;

         if (!DateTimeOffset.TryParse(time, null, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out dt))
            return null;
         return dt;
      }

      // URL = https://myaccount.blob.core.windows.net/music:           canonicalizedresource = "/myaccount/music"
      // URL = https://myaccount.blob.core.windows.net/music/intro.mp3: canonicalizedresource = "/myaccount/music/intro.mp3"
      // URL = https://myaccount.queue.core.windows.net/thumbnails:     canonicalizedresource = "/myaccount/thumbnails"
      // URL = https://myaccount.table.core.windows.net/Employees(PartitionKey='Jeff',RowKey='Price') canonicalizedresource = "/myaccount/employees"
      private static String GetCanonicalName(String account, String containerTableOrQueueName, String blobName = null) {
         // For container & queue: return "/" + accountName + absolutePath;
         // For a blob
         String str = "/" + account + "/" + containerTableOrQueueName.ToLowerInvariant();
         if (blobName != null) str += "/" + blobName.Replace('\\', '/');
         return str;
      }

      /// <summary>Creates a SAS string for a blob container.</summary>
      /// <param name="accountName">Identifies the Azure storage account's name.</param>
      /// <param name="accountKey">Identifies the Azure storage accounts key.</param>
      /// <param name="version">The SAS version.</param>
      /// <param name="containerName">The name of the blob container.</param>
      /// <param name="signatureIdentifier">An optional signature identifier.</param>
      /// <param name="permissions">The desired permissions.</param>
      /// <param name="startTime">The optional start time.</param>
      /// <param name="expiryTime">The optional end time.</param>
      /// <returns>The SAS string.</returns>
      public static String CreateContainerSas(String accountName, IBuffer accountKey, String version, String containerName,
         String signatureIdentifier, SasContainerPermissions permissions, DateTimeOffset? startTime, DateTimeOffset? expiryTime) {
         String canonicalResourceName = GetCanonicalName(accountName, containerName);
         return CreateSas(accountKey, version, "c", canonicalResourceName, signatureIdentifier,
            (UInt32)permissions, startTime, expiryTime);
      }

      /// <summary>Creates a SAS string for a blob.</summary>
      /// <param name="accountName">Identifies the Azure storage account's name.</param>
      /// <param name="accountKey">Identifies the Azure storage accounts key.</param>
      /// <param name="version">The SAS version.</param>
      /// <param name="containerName">The name of the blob container.</param>
      /// <param name="blobName">The name of the blob within the container.</param>
      /// <param name="signatureIdentifier">An optional signature identifier.</param>
      /// <param name="permissions">The desired permissions.</param>
      /// <param name="startTime">The optional start time.</param>
      /// <param name="expiryTime">The optional end time.</param>
      /// <returns>The SAS string.</returns>
      public static String CreateBlobSas(String accountName, IBuffer accountKey, String version, String containerName, String blobName,
         String signatureIdentifier, SasBlobPermissions permissions, DateTimeOffset? startTime, DateTimeOffset? expiryTime) {
         String canonicalResourceName = GetCanonicalName(accountName, containerName, blobName);
         return CreateSas(accountKey, version, "b", canonicalResourceName, signatureIdentifier,
            (UInt32)permissions, startTime, expiryTime);
      }

      /// <summary>Creates a SAS string for a queue.</summary>
      /// <param name="accountName">Identifies the Azure storage account's name.</param>
      /// <param name="accountKey">Identifies the Azure storage accounts key.</param>
      /// <param name="version">The SAS version.</param>
      /// <param name="queueName">The name of the queue.</param>
      /// <param name="signatureIdentifier">An optional signature identifier.</param>
      /// <param name="permissions">The desired permissions.</param>
      /// <param name="startTime">The optional start time.</param>
      /// <param name="expiryTime">The optional end time.</param>
      /// <returns>The SAS string.</returns>
      public static String CreateQueueSas(String accountName, IBuffer accountKey, String version, String queueName,
         String signatureIdentifier, SasQueuePermissions permissions, DateTimeOffset? startTime, DateTimeOffset? expiryTime) {
         String canonicalResourceName = GetCanonicalName(accountName, queueName);
         return CreateSas(accountKey, version, null, canonicalResourceName, signatureIdentifier,
            (UInt32)permissions, startTime, expiryTime);
      }
      /// <summary>Creates a SAS string for a table.</summary>
      /// <param name="accountName">Identifies the Azure storage account's name.</param>
      /// <param name="accountKey">Identifies the Azure storage accounts key.</param>
      /// <param name="version">The SAS version.</param>
      /// <param name="tableName">The name of the table.</param>
      /// <param name="signatureIdentifier">An optional signature identifier.</param>
      /// <param name="permissions">The desired permissions.</param>
      /// <param name="startTime">The optional start time.</param>
      /// <param name="expiryTime">The optional end time.</param>
      /// <param name="startPartitionKey">The optional start partition key.</param>
      /// <param name="startRowKey">The optional start row key.</param>
      /// <param name="endPartitionKey">The optional end partition key.</param>
      /// <param name="endRowKey">The optional end row key.</param>
      /// <returns>The SAS string.</returns>
      public static String CreateTableSas(String accountName, IBuffer accountKey, String version, String tableName,
         String signatureIdentifier, SasTablePermissions permissions, DateTimeOffset? startTime, DateTimeOffset? expiryTime,
         String startPartitionKey = null, String startRowKey = null, String endPartitionKey = null, String endRowKey = null) {
         String canonicalResourceName = GetCanonicalName(accountName, tableName);
         return CreateSas(accountKey, version, null, canonicalResourceName, signatureIdentifier,
            (UInt32)permissions, startTime, expiryTime,
            tableName, startPartitionKey, startRowKey, endPartitionKey, endRowKey);
      }
      private static String ToPermissionString(UInt32 permissions) {
         String perm = String.Empty;
         for (Char c = (Char)(permissions & 0xff); permissions != 0; c = (Char)((permissions >>= 8) & 0xff))
            if (c != 0) perm += c.ToString();
         return perm;
      }

      private static String GetDateTimeOrEmpty(DateTimeOffset? value) {
         return value.HasValue ? value.Value.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture) : String.Empty;
      }
      private static String CreateSas(IBuffer accountKey, String version,
         String resourceType, String canonicalResourceName,
         String signatureIdentifier, UInt32 permissions, DateTimeOffset? startTime, DateTimeOffset? expiryTime,
         String tableName = null,
         String startPartitionKey = null, String startRowKey = null,
         String endPartitionKey = null, String endRowKey = null) {

         String signature;
#if true
         MacAlgorithmProvider mac = MacAlgorithmProvider.OpenAlgorithm(MacAlgorithmNames.HmacSha256);
         CryptographicHash hash = mac.CreateHash(accountKey);
         // String to sign: http://msdn.microsoft.com/en-us/library/azure/dn140255.aspx
         StringBuilder stringToSign = new StringBuilder();
         stringToSign.Append(ToPermissionString(permissions))
            .Append("\n").Append(GetDateTimeOrEmpty(startTime))
            .Append("\n").Append(GetDateTimeOrEmpty(expiryTime))
            .Append("\n").Append(canonicalResourceName)
            .Append("\n").Append(signatureIdentifier)
            .Append("\n").Append(version);
         if (version != "2012-02-12") {
            String CacheControl = String.Empty;       // rscc 
            String ContentDisposition = String.Empty; // rscd 
            String ContentEncoding = String.Empty;    // rsce 
            String ContentLanguage = String.Empty;    // rscl 
            String ContentType = String.Empty;        // rsct  
            stringToSign.Append("\n").Append(CacheControl)
               .Append("\n").Append(ContentDisposition)
               .Append("\n").Append(ContentEncoding)
               .Append("\n").Append(ContentLanguage)
               .Append("\n").Append(ContentType);
         }
         if (tableName != null) {
            stringToSign.Append("\n").Append(startPartitionKey)
               .Append("\n").Append(startRowKey)
               .Append("\n").Append(endPartitionKey)
               .Append("\n").Append(endRowKey);
         }
         hash.Append(stringToSign.ToString().Encode().AsBuffer());
         signature = CryptographicBuffer.EncodeToBase64String(hash.GetValueAndReset());
#else
         using (HashAlgorithm hashAlgorithm = new HMACSHA256(accountKey)) {
            String stringToSign = String.Format(CultureInfo.InvariantCulture, "{0}\n{1}\n{2}\n{3}\n{4}\n{5}",
               permissions, GetDateTimeOrEmpty(startTime), GetDateTimeOrEmpty(expiryTime),
               canonicalResourceName, signatureIdentifier, version);
            if (tableName != null)
               stringToSign = String.Format(CultureInfo.InvariantCulture, "{0}\n{1}\n{2}\n{3}\n{4}",
                  stringToSign, startPartitionKey, startRowKey, endPartitionKey, endRowKey);
            signature = Convert.ToBase64String(hashAlgorithm.ComputeHash(Encoding.UTF8.GetBytes(stringToSign)));
         }
#endif
         // NOTE: The order of query parameters is important
         return new StringBuilder()
            .Add("sv", version)
            .Add("st", startTime == null ? null : GetDateTimeOrEmpty(startTime))
            .Add("se", expiryTime == null ? null : GetDateTimeOrEmpty(expiryTime))
            .Add("sr", resourceType)
            .Add("tn", tableName)
            .Add("sp", ToPermissionString(permissions))
            .Add("spk", startPartitionKey).Add("srk", startRowKey)
            .Add("epk", endPartitionKey).Add("erk", endRowKey)
            .Add("si", signatureIdentifier)
            .Add("sk", null/*accountKeyName*/)
            .Add("sig", signature).ToString();
      }
      private static StringBuilder Add(this StringBuilder sb, String key, String value) {
         if (String.IsNullOrWhiteSpace(value)) return sb;
         sb.Append(sb.Length == 0 ? "?" : "&"); // delimiter
         sb.Append(key + "=" + Uri.EscapeDataString(value));
         return sb;
      }
   }
}