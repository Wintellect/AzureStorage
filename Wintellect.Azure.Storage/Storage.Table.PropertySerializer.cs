/******************************************************************************
Module:  Storage.Table.PropertySerializer.cs
Notices: Copyright (c) by Jeffrey Richter & Wintellect
******************************************************************************/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Wintellect.Azure.Storage.Table {
   /// <summary>Use this class to serialize a collection of records to an Azure table property.</summary>
   public sealed class PropertySerializer {
      /// <summary>Serializes a collection of records to an Azure table property.</summary>
      /// <typeparam name="TRecord">The type of records in the collection.</typeparam>
      /// <param name="maxRecordsPerEntity">The maximum number of records to serialize.</param>
      /// <param name="version">The version number of the serialization format.</param>
      /// <param name="records">The collection of records.</param>
      /// <param name="approxRecordSize">The approximate record size in bytes. This is a hint to improve performance.</param>
      /// <param name="recordToProperty">The method that serializes a single record to bytes.</param>
      /// <returns>The array of bytes containing the serialized records.</returns>
      public static Byte[] ToProperty<TRecord>(Int32 maxRecordsPerEntity, UInt16 version, 
         IEnumerable<TRecord> records, Int32 approxRecordSize, 
         Action<TRecord, UInt16, PropertySerializer> recordToProperty) {
         Int32 maxRecords = Math.Min(records.Count() + 1, maxRecordsPerEntity);
         var serializer = new PropertySerializer(records.Count() * approxRecordSize).Add(version);  // Put version number in
         Int32 count = 0;
         foreach (TRecord record in records) {
            if (count++ >= maxRecords) break; // Don't go above 'maxRecords'
            recordToProperty(record, version, serializer);
         }
         return serializer.ToArray();
      }

      private static readonly Encoding s_utf8 = Encoding.UTF8;
      private Byte[] m_bytes;
      private Int32 m_size = 0;
      
      /// <summary>Returns the number of bytes in serialized into the byte array.</summary>
      public Int32 Size { get { return m_size; } }

      /// <summary>Gets and sets the byte array's capacity.</summary>
      public Int32 Capacity {
         get { return m_bytes.Length; }
         set {
            if (value != m_bytes.Length) {
               if (value < m_size) {
                  throw new ArgumentOutOfRangeException("Attempt to set capacity smaller than what's currently in use.");
               }
               Array.Resize(ref m_bytes, value);
            }
         }
      }
      /// <summary>Constructs a PropertySerializer setting its initial capacity.</summary>
      /// <param name="initialCapacity">The initialize size of the internal byte array which the records get serialized into.</param>
      public PropertySerializer(Int32 initialCapacity) { m_bytes = new Byte[initialCapacity]; }
      private void EnsureCapacity(Int32 min) {
         if (m_bytes.Length < min) Capacity = Math.Max(min, m_bytes.Length * 2);
      }
      private PropertySerializer Add(UInt64 value, Int32 numBytes) {
         EnsureCapacity(m_size + numBytes);
         for (Int32 n = 0; n < numBytes; n++) {
            m_bytes[m_size++] = (Byte)(value & 0xff);
            value >>= 8;
         }
         return this;
      }

      /// <summary>Serializes the value into the byte array.</summary>
      /// <param name="value">The value to serialize.</param>
      /// <returns>The same PropertySerializer so you can chain Add calls together.</returns>
      public PropertySerializer Add(Byte value) { return Add(value, sizeof(Byte)); }
      /// <summary>Serializes the value into the byte array.</summary>
      /// <param name="value">The value to serialize.</param>
      /// <returns>The same PropertySerializer so you can chain Add calls together.</returns>
      public PropertySerializer Add(Int16 value) { return Add(unchecked((UInt64)value), sizeof(Int16)); }
      /// <summary>Serializes the value into the byte array.</summary>
      /// <param name="value">The value to serialize.</param>
      /// <returns>The same PropertySerializer so you can chain Add calls together.</returns>
      public PropertySerializer Add(UInt16 value) { return Add(value, sizeof(UInt16)); }
      /// <summary>Serializes the value into the byte array.</summary>
      /// <param name="value">The value to serialize.</param>
      /// <returns>The same PropertySerializer so you can chain Add calls together.</returns>
      public PropertySerializer Add(Int32 value) { return Add(unchecked((UInt64)value), sizeof(Int32)); }
      /// <summary>Serializes the value into the byte array.</summary>
      /// <param name="value">The value to serialize.</param>
      /// <returns>The same PropertySerializer so you can chain Add calls together.</returns>
      public PropertySerializer Add(UInt32 value) { return Add(value, sizeof(UInt32)); }
      /// <summary>Serializes the value into the byte array.</summary>
      /// <param name="value">The value to serialize.</param>
      /// <returns>The same PropertySerializer so you can chain Add calls together.</returns>
      public PropertySerializer Add(Int64 value) { return Add(unchecked((UInt64)value), sizeof(Int64)); }
      /// <summary>Serializes the value into the byte array.</summary>
      /// <param name="value">The value to serialize.</param>
      /// <returns>The same PropertySerializer so you can chain Add calls together.</returns>
      public PropertySerializer Add(UInt64 value) { return Add(unchecked(value), sizeof(Int64)); }
      /// <summary>Serializes the value into the byte array.</summary>
      /// <param name="value">The value to serialize.</param>
      /// <param name="maxChars">The maximum characters you expect any string.</param>
      /// <returns>The same PropertySerializer so you can chain Add calls together.</returns>
      public PropertySerializer Add(String value, Int32 maxChars) {
         if (maxChars < 1 || maxChars > 65535)
            throw new ArgumentOutOfRangeException("maxChars", "maxChars must >0 and <=65535");
         if (value.Length > maxChars)
            throw new ArgumentException(String.Format("value > {0} characters", maxChars.ToString()));

         Int32 byteCount = s_utf8.GetByteCount(value);
         Add(unchecked((UInt32) byteCount), maxChars < 256 ? sizeof(Byte) : sizeof(UInt16));
         EnsureCapacity(m_size + byteCount);
         s_utf8.GetBytes(value, 0, value.Length, m_bytes, m_size);
         m_size += byteCount;
         return this;
      }
      /// <summary>Returns the byte array containing the serialized collection of records./// </summary>
      public Byte[] ToArray() { Capacity = m_size; return m_bytes; }
   }

   /// <summary>Use this class to deserialize a collection of records from an Azure table property.</summary>
   public sealed class PropertyDeserializer {
      /// <summary>Deserializes a collection of records from an Azure table property.</summary>
      /// <typeparam name="TRecord">The type of records in the collection.</typeparam>
      /// <param name="propertyBytes">The Azure table property's byte array.</param>
      /// <param name="propertyToRecord">The method that deserializes bytes to a single record.</param>
      /// <returns>The collection of records.</returns>
      public static List<TRecord> FromProperty<TRecord>(Byte[] propertyBytes, Func<UInt16, PropertyDeserializer, TRecord> propertyToRecord)  {
         var records = new List<TRecord>();
         if (propertyBytes == null || propertyBytes.Length == 0) return records;

         var deserializer = new PropertyDeserializer(propertyBytes);
         UInt16 version = deserializer.ToUInt16();

         // If version != 0, this property is before we had version #s at all, so upgrade to version-supporting
         while (deserializer.BytesRemaining > 0)
            records.Add(propertyToRecord(version, deserializer));
         return records;
      }

      private readonly Byte[] m_bytes;
      private Int32 m_offset = 0;
      /// <summary>Constructs a PropertyDeserializer over a byte array and starting offset within the byte array.</summary>
      /// <param name="bytes">The byte array to deserialize records from.</param>
      /// <param name="startOffset">The starting offset into the byte array.</param>
      public PropertyDeserializer(Byte[] bytes, Int32 startOffset = 0) { m_bytes = bytes; m_offset = startOffset; }

      /// <summary>Returns the number of bytes remaining to be deserialized.</summary>
      public Int32 BytesRemaining { get { return m_bytes.Length - m_offset; } }
      //public PropertyDeserializer Backup(Int32 numBytes) { m_offset -= numBytes; return this; }

      /// <summary>Deserializes a value from the byte array.</summary>
      /// <returns>The deserialized value.</returns>
      public Byte ToByte() {
         Byte v = m_bytes[m_offset];
         m_offset += sizeof(Byte);
         return v;
      }
      /// <summary>Deserializes a value from the byte array.</summary>
      /// <returns>The deserialized value.</returns>
      public UInt16 ToUInt16() {
         UInt16 v = BitConverter.ToUInt16(m_bytes, m_offset);
         m_offset += sizeof(UInt16);
         return v;
      }
      /// <summary>Deserializes a value from the byte array.</summary>
      /// <returns>The deserialized value.</returns>
      public Int16 ToInt16() {
         Int16 v = BitConverter.ToInt16(m_bytes, m_offset);
         m_offset += sizeof(Int16);
         return v;
      }
      /// <summary>Deserializes a value from the byte array.</summary>
      /// <returns>The deserialized value.</returns>
      public UInt32 ToUInt32() {
         UInt32 v = BitConverter.ToUInt32(m_bytes, m_offset);
         m_offset += sizeof(UInt32);
         return v;
      }
      /// <summary>Deserializes a value from the byte array.</summary>
      /// <returns>The deserialized value.</returns>
      public Int32 ToInt32() {
         Int32 v = BitConverter.ToInt32(m_bytes, m_offset);
         m_offset += sizeof(Int32);
         return v;
      }
      /// <summary>Deserializes a value from the byte array.</summary>
      /// <returns>The deserialized value.</returns>
      public UInt64 ToUInt64() {
         UInt64 v = BitConverter.ToUInt64(m_bytes, m_offset);
         m_offset += sizeof(UInt64);
         return v;
      }
      /// <summary>Deserializes a value from the byte array.</summary>
      /// <returns>The deserialized value.</returns>
      public Int64 ToInt64() {
         Int64 v = BitConverter.ToInt64(m_bytes, m_offset);
         m_offset += sizeof(UInt64);
         return v;
      }
      /// <summary>Deserializes a value from the byte array.</summary>
      /// <param name="maxChars">The maximum number of characters in the string.</param>
      /// <returns>The deserialized value.</returns>
      public String ToString(Int32 maxChars) {
         if (maxChars < 1 || maxChars > 65535)
            throw new ArgumentOutOfRangeException("maxChars", "maxChars must >0 and <65536");

         Int32 encodedLength = (maxChars < 256) ? ToByte() : ToInt16();
         String v = Encoding.UTF8.GetString(m_bytes, m_offset, encodedLength);
         m_offset += encodedLength;
         return v;
      }
   }
}
