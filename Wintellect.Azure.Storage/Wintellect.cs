/******************************************************************************
Module:  Wintellect.cs
Notices: Copyright (c) by Jeffrey Richter & Wintellect
******************************************************************************/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using System.Security.Cryptography.Pkcs;
using System.Security.Cryptography.X509Certificates;
using System.Text;

#if false
namespace Wintellect {
   public static class Diagnostics {
      [Conditional("TRACE"), MethodImpl(MethodImplOptions.NoInlining), DebuggerStepThrough]
      public static void WhenWhere(Action<StringBuilder> callback, [CallerMemberName] String member = null, [CallerFilePath] String file = null, [CallerLineNumber] Int32 line = 0) {
         var sb = new StringBuilder("[")
            .Append(DateTimeOffset.UtcNow.ToString()).Append(" ")
            .Append(member).Append(" ")
            .Append(file).Append(" ")
            .Append(line.ToString()).Append("] ");
         callback(sb);
      }
   }
}
#endif

namespace Wintellect {
   /// <summary>Defines String extension methods.</summary>
   public static class StringEx {
      /// <summary>Indicates whether a specified string is null, empty, or consists only of white-space characters.</summary>
      /// <param name="s">The string to test.</param>
      /// <returns>true if the value parameter is null or System.String.Empty, or if value consists exclusively of white-space characters.</returns>
      [DebuggerStepThrough]
      public static Boolean IsNullOrWhiteSpace(this String s) { return String.IsNullOrWhiteSpace(s); }

      /// <summary>Converts a base-64 string to a byte array.</summary>
      /// <param name="s">The base-64 string to convert.</param>
      /// <returns>The byte array.</returns>
      [DebuggerStepThrough]
      public static Byte[] FromBase64ToBytes(this String s) { return Convert.FromBase64String(s); }
      /// <summary>Converts a byte array to a base-64 string.</summary>
      /// <param name="data">The byte array to convert.</param>
      /// <returns>The base-64 string.</returns>
      [DebuggerStepThrough]
      public static String ToBase64String(this Byte[] data) { return Convert.ToBase64String(data); }

      /// <summary>Converts an Int32 to a base-64 string.</summary>
      /// <param name="value">The Int32 to convert.</param>
      /// <returns>The base-64 string.</returns>
      [DebuggerStepThrough]
      public static String ToBase64(this Int32 value) {
         // Convert 4-byte Int32 to Byte[4] and Base-64 encode it
         return Convert.ToBase64String(BitConverter.GetBytes(value));   // Byte[<=64]
      }

      /// <summary>Converts a base-64 string to an Int32.</summary>
      /// <param name="value">The base-64 string to convert.</param>
      /// <returns>The Int32.</returns>
      [DebuggerStepThrough]
      public static Int32 FromBase64ToInt32(this String value) {
         return BitConverter.ToInt32(Convert.FromBase64String(value), 0);
      }

      /// <summary>HTML-encodes a string.</summary>
      /// <param name="s">The string to HTML encode.</param>
      /// <returns>The HTML-encoded string.</returns>
      [DebuggerStepThrough]
      public static String HtmlEncode(this String s) { return System.Web.HttpUtility.HtmlEncode(s); }

      /// <summary>Encodes a string to a byte array.</summary>
      /// <param name="s">The string to encode.</param>
      /// <param name="encoding">The encoding to use (default is UTF-8).</param>
      /// <returns>The encoded string as a byte array.</returns>
      [DebuggerStepThrough]
      public static Byte[] Encode(this String s, Encoding encoding = null) { return (encoding ?? Encoding.UTF8).GetBytes(s); }

      /// <summary>Decodes a byte array to a string.</summary>
      /// <param name="data">The byte array to decode.</param>
      /// <param name="encoding">The encoding to use (default is UTF-8).</param>
      /// <returns>The decoded string.</returns>
      [DebuggerStepThrough]
      public static String Decode(this Byte[] data, Encoding encoding = null) { return (encoding ?? Encoding.UTF8).GetString(data); }
      /// <summary>Decodes a byte array to a string.</summary>
      /// <param name="data">The byte array to decode.</param>
      /// <param name="index">The starting index within the byte array.</param>
      /// <param name="count">The number of bytes to decode.</param>
      /// <param name="encoding">The encoding to use (default is UTF-8).</param>
      /// <returns>The decoded string.</returns>
      [DebuggerStepThrough]
      public static String Decode(this Byte[] data, Int32 index, Int32 count, Encoding encoding = null) { return (encoding ?? Encoding.UTF8).GetString(data, index, count); }

      /// <summary>Performs a case-insensitive comparison against the start of a string.</summary>
      /// <param name="string">The string to compare against.</param>
      /// <param name="prefix">The prefix you want to see if string starts with.</param>
      /// <returns>true if string starts with prefix (in a case insensitive way).</returns>
      public static Boolean StartsWithInsensitive(this String @string, String prefix) {
         return String.Compare(@string, 0, prefix, 0, prefix.Length, StringComparison.OrdinalIgnoreCase) == 0;
      }

      /// <summary>Returns a comma-separated string consisting of the elements. Each string element is enclosed in quotes.</summary>
      /// <param name="elements">The elements to quote and comma separate.</param>
      /// <returns>The CSV string.</returns>
      public static String ToCsvLine(this IEnumerable<String> elements) {
         // This methods purposely does NOT accept Objects to avoid unnecessary boxings.
         // This forces callers to call ToString thereby reducing memory overhead and improving performance.
         var sb = new StringBuilder();
         foreach (String e in elements) {
            if (sb.Length > 0) sb.Append(',');
            sb.Append('\"').Append(e).Append('\"');
         }
         return sb.AppendLine().ToString();
      }

      /// <summary>Returns a comma-separated string consisting of the elements. Each string element is enclosed in quotes.</summary>
      /// <param name="elements">The elements to quote and comma separate.</param>
      /// <returns>The CSV string.</returns>
      public static String ToCsvLine(params String[] elements) {
         // This methods purposely does NOT accept Objects to avoid unnecessary boxings.
         // This forces callers to call ToString thereby reducing memory overhead and improving performance.
         return ToCsvLine((IEnumerable<String>)elements);
      }

      #region Cryptographic Message Syntax
      // Encrypts/decrypts messages via http://www.ietf.org/rfc/rfc3852.txt
      // CMS is used to digitally sign, digest, authenticate, or encrypt arbitrary message content.
      // CMS supports digital signatures & encryption. One encapsulation envelope can be nested
      // inside another. Likewise, one party can digitally sign some previously encapsulated data.  
      // It also allows arbitrary attributes, such as signing time, to be signed along with the message content,
      // and provides for other attributes such as countersignatures to be associated with a signature.

      /// <summary>Encrypts a string using the Cryptographic Message Syntax.</summary>
      /// <param name="clearText">The string data to encrypt.</param>
      /// <param name="certificate">The certificate (must be marked for key exchange).</param>
      /// <returns>The encrypted string.</returns>
      public static String EncryptTextUsingCms(this String clearText, X509Certificate2 certificate) {
         // Good way to get a certificate: new X509Certificate2(pfxCertificatePathname, pfxPassword);

         // Create an envelope with the text as its contents
         var envelopedCms = new EnvelopedCms(new ContentInfo(Encoding.UTF8.GetBytes(clearText)));

         // Encrypt the envelope for the recipient & return the encoded value;
         // The recipient of the envelope is the owner of the certificate's private key
         // NOTE: the certificate info is embedded; no need to remember the thumbprint separately.
         envelopedCms.Encrypt(new CmsRecipient(certificate));
         return Convert.ToBase64String(envelopedCms.Encode());
      }

      /// <summary>Decrypts a string using the Cryptographic Message Syntax. The certificate (with private key) must be in the current user's or local compute's Personal (My) store or passed via the extraStore parameter.</summary>
      /// <param name="cipherText">The string data to decrypt.</param>
      /// <param name="extraStore">If not null, represents additional certificates to use for the decryption.</param>
      /// <returns>The decrypted string.</returns>
      public static String DecryptTextFromCms(this String cipherText, X509Certificate2Collection extraStore = null) {
         var envelopedCms = new EnvelopedCms();
         envelopedCms.Decode(Convert.FromBase64String(cipherText));
         if (extraStore == null) envelopedCms.Decrypt();
         else envelopedCms.Decrypt(extraStore);
         return Encoding.UTF8.GetString(envelopedCms.ContentInfo.Content);
      }
      #endregion
   }
}

namespace Wintellect {
   /// <summary>Defines methods that operate on enums.</summary>
   public static class EnumEx {
      /// <summary>Returns an enum's values.</summary>
      /// <typeparam name="TEnum">The enum type.</typeparam>
      /// <returns>The enum type's values.</returns>
      public static TEnum[] GetValues<TEnum>() where TEnum : struct {
         return (TEnum[])Enum.GetValues(typeof(TEnum));
      }
      /// <summary>Parses a string to an enum type's value.</summary>
      /// <typeparam name="TEnum">The enum type.</typeparam>
      /// <param name="value">The string symbol to parse.</param>
      /// <param name="ignoreCase">true to do a case-insensitive comparison.</param>
      /// <returns>The enum type's value.</returns>
      public static TEnum Parse<TEnum>(String value, Boolean ignoreCase = false) where TEnum : struct {
         return (TEnum)Enum.Parse(typeof(TEnum), value, ignoreCase);
      }
   }
}

namespace Wintellect {
   /// <summary>A helper struct that returns the string name for a type's property.</summary>
   /// <typeparam name="T">The type defining the property you wish to convert to a string.</typeparam>
   public struct PropertyName<T> {
      /// <summary>Returns a type's property name as a string.</summary>
      /// <param name="propertyExpression">An expression that returns the desired property.</param>
      /// <returns>The property name as a string.</returns>
      public String this[LambdaExpression propertyExpression] {
         get {
            Expression body = propertyExpression.Body;
            MemberExpression me = (body is UnaryExpression)
               ? (MemberExpression)((UnaryExpression)body).Operand
               : (MemberExpression)body;
            return me.Member.Name;
         }
      }

      /// <summary>Returns a type's property name as a string.</summary>
      /// <param name="propertyExpression">An expression that returns the desired property.</param>
      /// <returns>The property name as a string.</returns>
      public String this[Expression<Func<T, Object>> propertyExpression] {
         get {
            return this[(LambdaExpression)propertyExpression];
         }
      }

      /// <summary>Returns several types property names as a string collection.</summary>
      /// <param name="propertyExpressions">The expressions; each returns a desired property.</param>
      /// <returns>The property names as a string collection.</returns>
      public String[] this[params Expression<Func<T, Object>>[] propertyExpressions] {
         get {
            var propertyNames = new String[propertyExpressions.Length];
            for (Int32 i = 0; i < propertyNames.Length; i++) propertyNames[i] = this[(LambdaExpression)propertyExpressions[i]];
            return propertyNames;
         }
      }
   }
}

namespace Wintellect {
   /// <summary>The class defines guid extension methods.</summary>
   public static class GuidExtensions {
      /// <summary>Write a Guid to a BinaryWriter.</summary>
      /// <param name="stream">The BinaryWriter object.</param>
      /// <param name="value">The Guid to write.</param>
      public static void Write(this BinaryWriter stream, Guid value) {
         GuidBytes gb = value.ToBytes();
         stream.Write(gb.UInt64_0);
         stream.Write(gb.UInt64_1);
      }
      /// <summary>Reads a Guid from a BinaryWriter.</summary>
      /// <param name="stream">The BinaryWriter.</param>
      /// <returns>The Guid.</returns>
      public static Guid ReadGuid(this BinaryReader stream) {
         return new GuidBytes(stream.ReadUInt64(), stream.ReadUInt64()).Guid;
      }
      /// <summary>Efficiently returns a Guid's bytes without making any memory allocations.</summary>
      /// <param name="guid">The Guid whose bytes you wish to obtain.</param>
      /// <returns>A GuidBytes allowing you to get the Guid's byte values.</returns>
      public static GuidBytes ToBytes(this Guid guid) { return new GuidBytes(guid); }
   }

   /// <summary>Efficiently manipulates the byes within a Guid instance.</summary>
   [StructLayout(LayoutKind.Explicit)]
   public struct GuidBytes {
      /// <summary>Returns the Guid as a Guid.</summary>
      [FieldOffset(0)]
      public readonly Guid Guid;

      /// <summary>Returns the first 64 bits within the Guid .</summary>
      [FieldOffset(0)]
      public readonly UInt64 UInt64_0;
      /// <summary>Returns the second 64 bits within the Guid .</summary>
      [FieldOffset(8)]
      public readonly UInt64 UInt64_1;

      /// <summary>Returns the a part of the Guid .</summary>
      [FieldOffset(0)]
      public readonly uint a;
      /// <summary>Returns the b part of the Guid .</summary>
      [FieldOffset(4)]
      public readonly ushort b;
      /// <summary>Returns the c part of the Guid .</summary>
      [FieldOffset(6)]
      public readonly ushort c;
      /// <summary>Returns the d part of the Guid .</summary>
      [FieldOffset(8)]
      public readonly byte d;
      /// <summary>Returns the e part of the Guid .</summary>
      [FieldOffset(9)]
      public readonly byte e;
      /// <summary>Returns the f part of the Guid .</summary>
      [FieldOffset(10)]
      public readonly byte f;
      /// <summary>Returns the g part of the Guid .</summary>
      [FieldOffset(11)]
      public readonly byte g;
      /// <summary>Returns the h part of the Guid .</summary>
      [FieldOffset(12)]
      public readonly byte h;
      /// <summary>Returns the i part of the Guid .</summary>
      [FieldOffset(13)]
      public readonly byte i;
      /// <summary>Returns the j part of the Guid .</summary>
      [FieldOffset(14)]
      public readonly byte j;
      /// <summary>Returns the k part of the Guid .</summary>
      [FieldOffset(15)]
      public readonly byte k;

      /// <summary>Wraps a Guid instance with a GuidBytes instance allowing efficient access to the Guid's bytes.</summary>
      /// <param name="guid">The Guid to wrap.</param>
      public GuidBytes(Guid guid)
         : this() {
         Guid = guid;
      }
      /// <summary>Creates a GuidBytes instance from a Guid's two 64-bit integer values.</summary>
      /// <param name="UInt64_0">The first 64 bits of the Guid.</param>
      /// <param name="UInt64_1">The second 64 bits of the Guid.</param>
      public GuidBytes(UInt64 UInt64_0, UInt64 UInt64_1)
         : this() {
         this.UInt64_0 = UInt64_0;
         this.UInt64_1 = UInt64_1;
      }
   }
}