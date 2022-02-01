using System;
using System.Globalization;

namespace Azure.EntityServices.Queries
{
    internal static class ObjectExtensions
    {
        public static string ToInvariantString(this object value)
        {
            return value switch
            {
                byte[] v => Convert.ToBase64String(v),
                DateTime v => v.ToString("o", CultureInfo.InvariantCulture),
                DateTimeOffset v => v.ToString("o", CultureInfo.InvariantCulture),
                _ => Convert.ToString(value, CultureInfo.InvariantCulture)
            };
        }
    }
}