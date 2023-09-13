using Azure.Data.Tables;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Core
{
    public interface ITableBatchClient
    {
        decimal OutstandingOperations { get; }

        Task CommitTransactionAsync();
        void Delete<TEntity>(TEntity entity) where TEntity : ITableEntity;
        void Insert<TEntity>(TEntity entity) where TEntity : ITableEntity;
        void InsertOrMerge<TEntity>(TEntity entity) where TEntity : ITableEntity;
        void InsertOrReplace<TEntity>(TEntity entity) where TEntity : ITableEntity;
        void Merge<TEntity>(TEntity entity) where TEntity : ITableEntity;
        void Replace<TEntity>(TEntity entity) where TEntity : ITableEntity;
        Task SubmitToPipelineAsync(string partitionKey, CancellationToken cancellationToken = default);
        Task SubmitToStorageAsync(CancellationToken cancellationToken = default);
    }
}