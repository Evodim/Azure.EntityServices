using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Core.Abstractions
{
    public interface ITableBatchClient<T> where T : class, new()
    {
        decimal OutstandingOperations { get; }

        void AddOperation(EntityOperationType entityOperationType, T entity);

        Task CompletePipelineAsync(); 

        Task SendToPipelineAsync(string partitionKey, CancellationToken cancellationToken = default);

        Task SubmitAsync(CancellationToken cancellationToken = default);
    }
}