using Azure.EntityServices.Queries;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Core.Abstractions
{
    public interface ITableClient<T> where T : class, new()
    {
        Task<bool> CreateTableIfNotExists(CancellationToken cancellationToken = default);
        Task<bool> DropTableIfExists(CancellationToken cancellationToken = default);
        Task<IDictionary<string, object>> GetEntityProperties(string partitionKey, string rowKey, IEnumerable<string> properties = null, CancellationToken cancellationToken = default);
        IAsyncEnumerable<EntityPage<T>> QueryEntities(Action<IQuery<T>> filter, int? maxPerPage, string nextPageToken, CancellationToken cancellationToken, bool? iterateOnly = false);
    }
}