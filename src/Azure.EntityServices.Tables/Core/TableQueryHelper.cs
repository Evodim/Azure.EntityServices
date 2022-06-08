using System;
using System.Globalization;
using System.Text;

namespace Azure.EntityServices.Tables.Core
{
    public static class TableQueryHelper
    {
        public static string ToRowKey<P>(string tagName, P value) =>
            $"{tagName}-{KeyValueToString(value)}";
         
        public static string ValueToString<P>(P givenValue)
        {
            return givenValue switch
            {
                bool v => v ? "true" : "false",
                float => Convert.ToString(givenValue, CultureInfo.InvariantCulture),
                decimal => Convert.ToString(givenValue, CultureInfo.InvariantCulture),
                double => Convert.ToString(givenValue,CultureInfo.InvariantCulture),
                int => Convert.ToString(givenValue, CultureInfo.InvariantCulture),
                long => string.Format("{0}L", givenValue),
                byte[] v => ByteArrayToString(v),
                DateTime v => string.Format("datetime'{0}'", (v==default)? TableConstants.DateTimeStorageDefault.ToString("o") :v.ToUniversalTime().ToString("o")),
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
                DateTime v => ((DateTimeOffset)v).UtcDateTime.ToString("o"),
                DateTimeOffset v => v.UtcDateTime.ToString("o"),
                bool v => v ? "true" : "false",
                double => Convert.ToString(givenValue),
                int => Convert.ToString(givenValue),
                long => string.Format("{0}L", givenValue),
                byte[] v => ByteArrayToString(v),
                Guid v => string.Format("{0}", v),
                BinaryData v => string.Format("X{0}", v),
                decimal v => Convert.ToString(v, CultureInfo.InvariantCulture),
                _ => givenValue == null ? "" : $"{givenValue}"
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