﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
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

        public static async IAsyncEnumerable<IEnumerable<T>> SkipAsync<T>(this IAsyncEnumerable<IEnumerable<T>> asyncEnumerableEntity,
            int skip,
           [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var skipped = 0;
            await foreach (var asyncEntity in asyncEnumerableEntity)
            {
                skipped += asyncEntity.Count();

                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException(cancellationToken);
                }
                if (skipped >= skip)
                {
                    
                    yield return asyncEntity.Skip(asyncEntity.Count() - (skipped - skip));
                    skipped = skip;
                }
                else 
                if (skipped == skip)
                {
                    yield return asyncEntity;
                }
            }
        }

        public static async IAsyncEnumerable<IEnumerable<T>> TakeAsync<T>(this IAsyncEnumerable<IEnumerable<T>> asyncEnumerableEntity,
          int take,
          [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var took = 0;
            await foreach (var asyncEntity in asyncEnumerableEntity)
            {
                took = took + asyncEntity.Count();

                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException(cancellationToken);
                }
                if (took < take)
                {
                    yield return asyncEntity;
                }
                else
                {
                    yield return asyncEntity.Take(asyncEntity.Count() - (took - take));
                    yield break;
                } 
            }
        }
    }
}