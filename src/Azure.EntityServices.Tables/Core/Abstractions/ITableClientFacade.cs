using Azure.EntityServices.Queries;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Core.Abstractions
{
    public interface ITableClientFacade<T> where T : class, new()
    {
        decimal OutstandingOperations { get; }

        void AddOperation(EntityOperationType entityOperationType, T entity);

        Task CompletePipelineAsync();

        Task SendOperations(string partitionKey, CancellationToken cancellationToken = default);

        Task SendOperation(CancellationToken cancellationToken = default);

        Task<bool> CreateTableIfNotExists(CancellationToken cancellationToken = default);

        Task<bool> DropTableIfExists(CancellationToken cancellationToken = default);

        Task<IDictionary<string, object>> GetEntityProperties(string partitionKey, string rowKey, IEnumerable<string> properties = null, CancellationToken cancellationToken = default);

        IAsyncEnumerable<EntityPage<T>> QueryEntities(Action<IQuery<T>> filter, int? maxPerPage, string nextPageToken, bool? iterateOnly = false, CancellationToken cancellationToken = default);
    }
}