using Azure.EntityServices.Queries;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables
{
    public interface IEntityTableClient<T>
    {
        Task AddAsync(T entity, CancellationToken cancellationToken = default);

        Task ReplaceAsync(T entity, CancellationToken cancellationToken = default);

        Task MergeAsync(T entity, CancellationToken cancellationToken = default);

        Task AddOrReplaceAsync(T entity, CancellationToken cancellationToken = default);

        Task AddOrReplaceManyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default);

        Task AddOrMergeAsync(T entity, CancellationToken cancellationToken = default);

        Task AddOrMergeManyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default);

        Task AddManyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default);

        Task<long> UpdateManyAsync(Action<T> updateAction, Action<IQuery<T>> filter = default, CancellationToken cancellationToken = default);

        Task<T> GetByIdAsync(string partition, object id, CancellationToken cancellationToken = default);

        IAsyncEnumerable<IEnumerable<T>> GetAsync(Action<IQuery<T>> filter = default, CancellationToken cancellationToken = default);

        Task<EntityPage<T>> GetPagedAsync(Action<IQuery<T>> filter = default, int? iteratorCount = null, int? maxPerPage = null, string nextPageToken = null, CancellationToken cancellationToken = default);

        Task<bool> DeleteByIdAsync(string partition, object id, CancellationToken cancellationToken = default);

        Task<bool> DeleteAsync(T entity, CancellationToken cancellationToken = default);

        Task DeleteManyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default);

        Task DropTableAsync(CancellationToken cancellationToken = default);

        Task CreateTableAsync(CancellationToken cancellationToken = default);
    }
}