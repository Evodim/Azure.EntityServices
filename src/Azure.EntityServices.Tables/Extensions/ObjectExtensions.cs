using System;
using System.Globalization;

namespace Azure.EntityServices.Tables.Extensions
{
    internal static class ObjectExtensions
    {
        
        public static string ToInvariantString<T>(this T value)
        {
            return value switch
            {
                DateTime v => (v == default) ? new DateTime(1, 1, 1, 0, 0, 0, DateTimeKind.Utc).ToString("o") : v.ToString("o", CultureInfo.InvariantCulture),
                DateTimeOffset v => (v == default) ? new DateTimeOffset(new DateTime(1, 1, 1, 0, 0, 0, DateTimeKind.Utc)).ToString("o", CultureInfo.InvariantCulture) : v.ToString("o", CultureInfo.InvariantCulture),
                _ => Convert.ToString(value, CultureInfo.InvariantCulture)
            };
        }
    }
}