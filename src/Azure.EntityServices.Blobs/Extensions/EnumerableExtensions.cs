using System.Collections.Generic;
using System.Linq;

namespace Azure.EntityServices.Blobs.Extensions
{
    public static class EnumerableExtensions
    {
        internal static IDictionary<T, U> AsDictionnary<T, U>(this IEnumerable<KeyValuePair<T, U>> items)
        {
            return items.ToDictionary(kv => kv.Key, kv => kv.Value);
        }
    }
}