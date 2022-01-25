using Azure.EntityServices.Queries;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Azure.EntityServices.Blobs
{
    public interface IEntityBlobClient<T>
    {
        Task<T> AddOrReplaceAsync(T entity); 

        Task<T> GetAsync(string entityRef);

        IAsyncEnumerable<IReadOnlyList<T>> ListAsync(string entityPath);

        IAsyncEnumerable<IReadOnlyList<T>> ListAsync(Action<IQueryCompose<T>> query);

        IAsyncEnumerable<IReadOnlyList<IDictionary<string, string>>> ListPropsAsync(string entityPath);

        Task<IDictionary<string, string>> GetPropsAsync(string entityRef); 

        string GetEntityReference(T entity); 

        Task<BinaryData> GetContentAsync(T entity);

        Task<BinaryData> GetContentAsync(string entityRef);

        Task DeleteAsync(string entityRef);

        Task DeleteAllAsync();

    }
}