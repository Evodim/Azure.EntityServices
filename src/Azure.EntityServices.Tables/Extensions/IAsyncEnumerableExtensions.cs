using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Extensions
{
    public static class IAsyncEnumerableExtensions
    {
        public static async Task<T> FirstAsync<T>(this IAsyncEnumerable<IEnumerable<T>> asyncEnumerableEntity)
        {
            var cursor = asyncEnumerableEntity.GetAsyncEnumerator();
            await cursor.MoveNextAsync();
            return cursor.Current.First();
        }
        public static async Task<IEnumerable<T>> FirstPageAsync<T>(this IAsyncEnumerable<IEnumerable<T>> asyncEnumerableEntity)
        {
            var cursor = asyncEnumerableEntity.GetAsyncEnumerator();
            await cursor.MoveNextAsync();

            return cursor.Current;

        }
        
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