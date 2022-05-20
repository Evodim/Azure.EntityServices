using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Extensions
{
    public static class IAsyncEnumerableExtensions
    {
        public static async Task ForEachAsync<T>(
            this IAsyncEnumerable<IEnumerable<T>> asyncEnumerable,
            Action<T> action,
            CancellationToken cancellationToken = default)
        {
            await using var enumerator = asyncEnumerable.GetAsyncEnumerator(cancellationToken);
            while (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                foreach (var item in enumerator.Current)
                {
                    action(item);
                }
            }
        }

        public static async Task<bool> AnyAsync<T>(
           this IAsyncEnumerable<IEnumerable<T>> asyncEnumerable,
           CancellationToken cancellationToken = default)
        {
            await using var enumerator = asyncEnumerable.GetAsyncEnumerator(cancellationToken);
            if (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                return enumerator.Current.Any();
            }
            return false;
        }

        public static async Task<T> FirstOrDefaultAsync<T>(this IAsyncEnumerable<IEnumerable<T>> asyncEnumerableEntity,
             CancellationToken cancellationToken = default)
        {
            await using var enumerator = asyncEnumerableEntity.GetAsyncEnumerator(cancellationToken);
            if (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                return enumerator.Current.FirstOrDefault();
            }
            return default;
        }

        public static async Task<T> FirstAsync<T>(this IAsyncEnumerable<IEnumerable<T>> asyncEnumerableEntity,
             CancellationToken cancellationToken = default)
        {
            await using var enumerator = asyncEnumerableEntity.GetAsyncEnumerator(cancellationToken);
            if (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                return enumerator.Current.First();
            }
            throw new InvalidOperationException("Enumerator returns no result");
        }

        public static async Task<IEnumerable<T>> FirstPageAsync<T>(this IAsyncEnumerable<IEnumerable<T>> asyncEnumerableEntity,
             CancellationToken cancellationToken = default)
        {
            await using var enumerator = asyncEnumerableEntity.GetAsyncEnumerator(cancellationToken);
            if (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                return enumerator.Current;
            }
            return Enumerable.Empty<T>();
        }

        public static async Task<List<T>> ToListAsync<T>(this IAsyncEnumerable<IEnumerable<T>> asyncEnumerableEntity,
              CancellationToken cancellationToken = default)
        {
            var result = new List<T>();
            await foreach (var asyncEntity in asyncEnumerableEntity)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException(cancellationToken);
                }
                result.AddRange(asyncEntity);
            }
            return result;
        }
    }
}