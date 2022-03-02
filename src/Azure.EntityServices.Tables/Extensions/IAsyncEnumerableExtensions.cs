using System.Collections.Generic;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Extensions
{
    public static class IAsyncEnumerableExtensions
    {
        public static async Task<IEnumerable<T>> AllAsync<T>(this IAsyncEnumerable<IEnumerable<T>> asyncEnumerableEntity)
        {
            var result = new List<T>();
            await foreach (var asyncEntity in asyncEnumerableEntity)
            {
                result.AddRange(asyncEntity);
            }
            return result;
        }
    }
}