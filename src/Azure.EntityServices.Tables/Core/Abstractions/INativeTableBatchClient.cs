using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Core.Abstractions
{
    public interface INativeTableBatchClient<T> where T : class, new()
    {
        decimal OutstandingOperations { get; }

        Task CommitTransactionAsync();

        void Delete(T entity);

        void Insert(T entity);

        void InsertOrMerge(T entity);

        void InsertOrReplace(T entity);

        void Merge(T entity);

        void Replace(T entity);

        Task SubmitToPipelineAsync(string partitionKey, CancellationToken cancellationToken = default);

        Task SubmitToStorageAsync(CancellationToken cancellationToken = default);
    }
}