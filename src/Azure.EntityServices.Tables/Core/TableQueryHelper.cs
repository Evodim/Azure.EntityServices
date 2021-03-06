using Azure.EntityServices.Tables.Extensions;
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
                float => $"'{givenValue.ToInvariantString()}'",
                decimal =>$"'{givenValue.ToInvariantString()}'" ,
                double => givenValue.ToInvariantString(),
                int => givenValue.ToInvariantString(),
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