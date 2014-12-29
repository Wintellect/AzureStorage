using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Wintellect.Periodic;

namespace Wintellect.Azure.Storage.Blob {
   /// <summary>The base class for on-going and periodic log blobs.</summary>
   public abstract class BlobLogBase {
      /// <summary>The container holding the log blob.</summary>
      protected readonly CloudBlobContainer m_container;
      private readonly AppendBlobBlockInfo m_info;
      private readonly String m_extension;

      /// <summary>Constructs a BlobLogBase object.</summary>
      /// <param name="container">Indicates the container where log blobs should be placed.</param>
      /// <param name="extension">Indicates the extension (initial period is optional) of the log blob.</param>
      /// <param name="mimeType">Indicates the mime type of the log blob.</param>
      /// <param name="header">Indicates the header (first block) of the log blob. Pass 'null' if you do not want a header.</param>
      /// <param name="maxEntries">Indicates the maximum number of blocks the blob should have. Old blocks are deleted when going over this limit.</param>
      protected internal BlobLogBase(CloudBlobContainer container, String extension, String mimeType, Byte[] header = null, Int32 maxEntries = 49000) {
         m_container = container;
         m_extension = extension.StartsWith(".") ? extension : "." + extension;
         m_info = new AppendBlobBlockInfo { MimeType = mimeType, Header = header, MaxEntries = maxEntries };
      }

      /// <summary>Returns the CloudBlockBlob from the blob name.</summary>
      /// <param name="blobName">The blob's name.</param>
      /// <returns>The CloudBlockBlob.</returns>
      protected CloudBlockBlob GetBlob(String blobName) {
         CloudBlockBlob blob = m_container.GetBlockBlobReference(blobName + m_extension);
         return blob;
      }

      /// <summary>Appends a block to the end of the log blob.</summary>
      /// <param name="Id">An identifier identifying the main part of the blob's name.</param>
      /// <param name="blobName">The full name of the blob.</param>
      /// <param name="dataToAppend">The data to append as a block to the end of the log blob.</param>
      /// <param name="options">The blob request options.</param>
      /// <param name="operationContext">The operation context.</param>
      /// <param name="cancellationToken">The cancellation token.</param>
      /// <returns>A Task indicating when the operation completes.</returns>
      protected Task AppendBlockAsync(String Id, String blobName, Byte[] dataToAppend,
         BlobRequestOptions options = null, OperationContext operationContext = null,
         CancellationToken cancellationToken = default(CancellationToken)) {
         return GetBlob(blobName).AppendBlockAsync(m_info, dataToAppend, options, operationContext, cancellationToken);
      }

      /// <summary>An event that is raised whenever a new log blob is being created.</summary>
      public event EventHandler<NewBlobEventArgs> NewLog {
         add { m_info.NewBlob += value; }
         remove { m_info.NewBlob -= value; }
      }

      /// <summary>Calculates and returns a URI string (containing the blob URI and its SAS query string) for a log blob.</summary>
      /// <param name="blob">The blob to get a URI for.</param>
      /// <param name="sap">The SAS permissions to apply to the blob.</param>
      /// <returns>The URI (containing the blob URI and its SAS query string).</returns>
      public String GetSasUri(CloudBlockBlob blob, SharedAccessBlobPolicy sap) {
         // Make sure we use HTTPS so that SAS doesn't transfer in clear text ensuring security
         String uri = (blob.Uri.Scheme == Uri.UriSchemeHttps) ? blob.Uri.ToString() :
            new StringBuilder(Uri.UriSchemeHttps).Append("://").Append(blob.Uri.Host).Append(blob.Uri.PathAndQuery).ToString();
         return uri + blob.GetSharedAccessSignature(sap);
      }
   }

   /// <summary>Represents an on-going (not periodic) log blob.</summary>
   public sealed class BlobLog : BlobLogBase {
      /// <summary>Construct an on-going log blob.</summary>
      /// <param name="container">The container holding the log blob.</param>
      /// <param name="extension">Indicates the extension (initial period is optional) of the log blob.</param>
      /// <param name="mimeType">Indicates the mime type of the log blob.</param>
      /// <param name="header">Indicates the header (first block) of the log blob. Pass 'null' if you do not want a header.</param>
      /// <param name="maxEntries">Indicates the maximum number of blocks the blob should have. Old blocks are deleted when going over this limit.</param>
      public BlobLog(CloudBlobContainer container, String extension, String mimeType, Byte[] header, Int32 maxEntries = 49000) :
         base(container, extension, mimeType, header, maxEntries) {
      }

      /// <summary>Appends a block to the end of the log blob.</summary>
      /// <param name="Id">An identifier identifying the blob's name.</param>
      /// <param name="dataToAppend">The data to append as a block to the end of the log blob.</param>
      /// <returns>A Task indicating when the operation completes.</returns>
      public Task AppendAsync(String Id, Byte[] dataToAppend) {
         return base.AppendBlockAsync(Id, Id, dataToAppend);
      }

      /// <summary>Returns the CloudBlockBlob from the blob Id.</summary>
      /// <param name="Id">The log blob's Id.</param>
      /// <returns>The CloudBlockBlob.</returns>
      public new CloudBlockBlob GetBlob(String Id) { return base.GetBlob(Id); }
   }

   /// <summary>Used to indicate the naming convention (and therefore sort order) for PeriodBlobLogs.</summary>
   public enum PeriodBlobLogOrder { 
      /// <summary>Names/sorts PeriodBlobLogs by their Id.</summary>
      Id,
      /// <summary>Names/sorts PeriodBlobLogs by their time (chronological order).</summary>
      Time
   }


   /// <summary>Represents a periodic (not on-going) log blob.</summary>
   public sealed class PeriodBlobLog : BlobLogBase {
      private readonly Period m_period;
      private readonly PeriodBlobLogOrder m_logOrder;
      private readonly String[] m_delimiter;

      /// <summary>Construct an on-going log blob.</summary>
      /// <param name="period">Indicates the time period that a log blob should cover.</param>
      /// <param name="logOrder">Indicates the naming convention (sort order) used when naming periodic log blobs.</param>
      /// <param name="delimiter">Indicates the delimiter to use to separate the blob's name, period start date, and period end date.</param>
      /// <param name="container">The container holding the log blob.</param>
      /// <param name="extension">Indicates the extension (initial period is optional) of the log blob.</param>
      /// <param name="mimeType">Indicates the mime type of the log blob.</param>
      /// <param name="header">Indicates the header (first block) of the log blob. Pass 'null' if you do not want a header.</param>
      /// <param name="maxEntries">Indicates the maximum number of blocks the blob should have. Old blocks are deleted when going over this limit.</param>
      public PeriodBlobLog(Period period, PeriodBlobLogOrder logOrder, String delimiter,
         CloudBlobContainer container, String extension, String mimeType, Byte[] header, Int32 maxEntries = 49000) :
         base(container, extension, mimeType, header, maxEntries) {
         m_period = period;
         m_logOrder = logOrder;
         m_delimiter = new[] { delimiter };
      }

      private static String ToYMD(DateTimeOffset dto) { return dto.ToString("yyyyMMdd"); }
      private static DateTimeOffset FromYMD(String dto) { return DateTimeOffset.ParseExact(dto, "yyyyMMdd", null, DateTimeStyles.AssumeUniversal); }

      /// <summary>Returns the CloudBlockBlob from the blob's ID and cycle date.</summary>
      /// <param name="Id">The blob's identifier.</param>
      /// <param name="cycleDate">The date used to calculate the blob's name.</param>
      /// <returns>The CloudBlockBlob.</returns>
      public CloudBlockBlob GetBlob(String Id, DateTimeOffset cycleDate) {
         return base.GetBlob(GenerateBlobName(Id, cycleDate));
      }
      private String GenerateBlobName(String Id, DateTimeOffset cycleDate) {
         DateTimeOffset periodStop, periodStart = PeriodCalculator.CalculatePeriodDates(m_period, cycleDate, 0, out periodStop);
         StringBuilder blobName = new StringBuilder();
         String delimiter = m_delimiter[0];
         switch (m_logOrder) {
            case PeriodBlobLogOrder.Id:
               blobName.Append(Id).Append(delimiter)
                  .Append(ToYMD(periodStart)).Append(delimiter)
                  .Append(ToYMD(periodStop));
               break;
            case PeriodBlobLogOrder.Time:
               blobName.Append(ToYMD(periodStart)).Append(delimiter)
                  .Append(ToYMD(periodStop)).Append(delimiter)
                  .Append(Id);
               break;
         }
         return blobName.Append(delimiter).ToString();
      }

      /// <summary>A class containing information about a period log blob.</summary>
      public sealed class PeriodBlobLogNameInfo {
         internal PeriodBlobLogNameInfo(String id, DateTimeOffset periodStart, DateTimeOffset periodStop) {
            Id = id;
            PeriodStart = periodStart;
            PeriodStop = periodStop;
         }
         /// <summary>Indicates the log blob's ID.</summary>
         public readonly String Id;
         /// <summary>Indicates the period's starts date.</summary>
         public readonly DateTimeOffset PeriodStart;
         /// <summary>Indicates the period's end date.</summary>
         public readonly DateTimeOffset PeriodStop;
      }

      /// <summary>Parses a blob name returning its ID, and period start and end dates.</summary>
      /// <param name="blob">The blob whose name to parse.</param>
      /// <returns>The log blob's ID, period start and period end dates.</returns>
      public PeriodBlobLogNameInfo ParseLogName(CloudBlockBlob blob) {
         String[] tokens = blob.Name.Split(m_delimiter, StringSplitOptions.None);
         PeriodBlobLogNameInfo logInfo = null;
         switch (m_logOrder) {
            case PeriodBlobLogOrder.Id:
               logInfo = new PeriodBlobLogNameInfo(tokens[0], FromYMD(tokens[1]), FromYMD(tokens[2]));
               break;
            case PeriodBlobLogOrder.Time:
               logInfo = new PeriodBlobLogNameInfo(tokens[2], FromYMD(tokens[0]), FromYMD(tokens[1]));
               break;
         }
         return logInfo;
      }

      /// <summary>Appends a block to the end of the log blob.</summary>
      /// <param name="Id">An identifier identifying the blob's name.</param>
      /// <param name="cycleDate">The date used to determine which blob to append the data to.</param>
      /// <param name="dataToAppend">The data to append as a block to the end of the log blob.</param>
      /// <returns>A Task indicating when the operation completes.</returns>
      public Task AppendAsync(String Id, DateTimeOffset cycleDate, Byte[] dataToAppend) {
         String blobName = GenerateBlobName(Id, cycleDate);
         return base.AppendBlockAsync(Id, blobName, dataToAppend);
      }

      private String CalculatePrefix(String Id, DateTimeOffset startDate = default(DateTimeOffset)) {
         // If blobs are prefixed by Id, we scan the subset of blobs with this Id
         // If blobs are not prefixed by Id, we scan all blobs in this container
         if (m_logOrder == PeriodBlobLogOrder.Id) return (Id != null) ? (Id + m_delimiter[0]) : null;
         return ToYMD(startDate);
      }

      /// <summary>Deletes log blobs containing data before the specified date.</summary>
      /// <param name="Id">Identifies the log blob.</param>
      /// <param name="oldDate">The date before which log blobs should be deleted.</param>
      /// <returns>A Task indicating when the operation completes.</returns>
      public Task DeleteOldLogsAsync(String Id, DateTimeOffset oldDate) {
         return m_container.ScanBlobsAsync(blob => DeleteOldLogAsync(blob, oldDate), CalculatePrefix(Id));
      }

      private async Task<Boolean> DeleteOldLogAsync(CloudBlockBlob blob, DateTimeOffset oldDate) {
         PeriodBlobLogNameInfo lni = ParseLogName(blob);
         if (lni.PeriodStop < oldDate) await blob.DeleteAsync(); // Log data ends before old date, delete it
         return lni.PeriodStart > oldDate;  // Stop when we hit a log newer than the old date
      }

      /// <summary>Finds all log blobs with the specified ID containing data after the oldest date.</summary>
      /// <param name="Id">The ID of the log blobs to find.</param>
      /// <param name="oldest">The oldest date of blobs to consider.</param>
      /// <returns>A collection of CloudBlockBlob objects.</returns>
      public async Task<IEnumerable<CloudBlockBlob>> FindLogsAsync(String Id, DateTimeOffset oldest = default(DateTimeOffset)) {
         List<CloudBlockBlob> blobs = new List<CloudBlockBlob>();
         await m_container.ScanBlobsAsync(blob => {
            var lni = ParseLogName(blob);
            if (lni.Id == Id && lni.PeriodStart >= oldest) blobs.Add(blob);

            // If ordered by Id, it will stop when no more blobs match the Id prefix
            // If ordered by time, it will stop when all done
            return Task.FromResult(false);
         }, CalculatePrefix(Id, oldest));
         return blobs;
      }
   }
}