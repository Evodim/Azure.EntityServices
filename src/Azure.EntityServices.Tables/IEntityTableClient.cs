using Azure.EntityServices.Queries;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables
{
    public interface IEntityTableClientRuntimeConfig<T>
    {
        void AddObserver(string name, IEntityObserver<T> observer);

        void RemoveObserver(string name);
    }

    public interface IEntityTableClient<T> : IEntityTableClientRuntimeConfig<T>
    {
        Task AddAsync(T entity, CancellationToken cancellationToken = default);

        Task ReplaceAsync(T entity, CancellationToken cancellationToken = default);

        Task MergeAsync(T entity, CancellationToken cancellationToken = default);

        Task AddOrReplaceAsync(T entity, CancellationToken cancellationToken = default);

        Task AddOrMergeAsync(T entity, CancellationToken cancellationToken = default);

        Task AddManyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default);

        Task<T> GetByIdAsync(string partition, object id, CancellationToken cancellationToken = default);

        IAsyncEnumerable<IEnumerable<T>> GetAsync(Action<IQueryCompose<T>> filter = default, CancellationToken cancellationToken = default);

        IAsyncEnumerable<IEnumerable<T>> GetAsync(string partition, Action<IQueryCompose<T>> filter = default, CancellationToken cancellationToken = default);

        IAsyncEnumerable<IEnumerable<T>> GetByTagAsync<P>(Expression<Func<T, P>> tagProperty, P tagValue, Action<IQueryCompose<T>> filter = null, CancellationToken cancellationToken = default);

        IAsyncEnumerable<IEnumerable<T>> GetByTagAsync<P>(string partition, Expression<Func<T, P>> tagProperty, P tagValue, Action<IQueryCompose<T>> filter = null, CancellationToken cancellationToken = default);

        IAsyncEnumerable<IEnumerable<T>> GetByTagAsync(string tagProperty, object tagValue, Action<IQueryCompose<T>> filter = null, CancellationToken cancellationToken = default);

        IAsyncEnumerable<IEnumerable<T>> GetByTagAsync(string partition, string tagProperty, object tagValue, Action<IQueryCompose<T>> filter = null, CancellationToken cancellationToken = default);

        Task DeleteAsync(T entity, CancellationToken cancellationToken = default);

        Task DropTableAsync(CancellationToken cancellationToken = default);
    }
}