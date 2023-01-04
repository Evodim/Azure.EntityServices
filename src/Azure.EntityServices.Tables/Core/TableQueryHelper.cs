using Azure.EntityServices.Tables.Extensions;
using System;
using System.Text;
using System.Text.RegularExpressions;

namespace Azure.EntityServices.Tables.Core
{
    public static class TableQueryHelper
    {
        /// <summary>
        /// Escape characters disallowed in Azure Storage key fields
        /// https://learn.microsoft.com/en-us/rest/api/storageservices/understanding-the-table-service-data-model
        /// The forward slash(/) character
        /// The backslash(\) character
        /// The number sign(#) character
        /// The question mark (?) character
        /// Control characters from U+0000 to U+001F, including:
        /// The horizontal tab(\t) character
        /// The linefeed(\n) character
        /// The carriage return (\r) character
        /// Control characters from U+007F to U+009F
        /// <summary>

        /// <returns></returns>
        internal static string EscapeDisallowedChars(this string input)
        {
            if (string.IsNullOrWhiteSpace(input))
            {
                return input;
            }

            var exp = new Regex(@"([\/\\#\?\t\n\r\u0000-\u001f\u007f-\u009f]+)", RegexOptions.CultureInvariant);

            var matches = exp.Matches(input);
            foreach (var match in matches)
            {
                input = input.Replace((match as Match).Value, "*");
            }
            return input;
        }

        public static string ToPartitionKey(string value) =>
          value.EscapeDisallowedChars();

        public static string ToPrimaryRowKey<P>(P value) =>
           KeyValueToString(value).EscapeDisallowedChars();

        public static string ToTagRowKeyPrefix<P>(string tagName, P value) =>
            $"~{tagName}-{KeyValueToString(value)}$".EscapeDisallowedChars();

        public static string ValueToString<P>(P givenValue)
        {
            return givenValue switch
            {
                bool v => v ? "true" : "false",
                float => $"'{givenValue.ToInvariantString()}'",
                decimal => $"'{givenValue.ToInvariantString()}'",
                double => givenValue.ToInvariantString(),
                int => givenValue.ToInvariantString(),
                long => string.Format("{0}L", givenValue),
                byte[] v => ByteArrayToString(v),
                DateTime v => string.Format("datetime'{0}'", (v == default) ? TableConstants.DateTimeStorageDefault.ToString("o") : v.ToUniversalTime().ToString("o")),
                DateTimeOffset v => string.Format("datetime'{0}'", (v == default) ? new DateTimeOffset(TableConstants.DateTimeStorageDefault).UtcDateTime.ToString("o") : v.UtcDateTime.ToString("o")),
                Guid v => string.Format("guid'{0}'", v),
                BinaryData v => string.Format("X'{0}'", v),
                _ => givenValue == null ? "null" : $"'{givenValue}'"
            };
        }

        public static string KeyValueToString<P>(P givenValue)
        {
            return givenValue switch
            {
                bool v => v ? "true" : "false",
                long => string.Format("{0}L", givenValue),
                byte[] v => ByteArrayToString(v),
                Guid v => string.Format("{0}", v),
                BinaryData v => string.Format("X{0}", v),

                _ => givenValue == null ? "" : $"{givenValue.ToInvariantString()}"
            };
        }

        private static string ByteArrayToString(byte[] value)
        {
            var sb = new StringBuilder();
            foreach (byte b in value)
            {
                sb.AppendFormat("{0:x2}", b);
            }
            return sb.ToString();
        }
    }
}