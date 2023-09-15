using System.Threading.Tasks;
using System;
using Azure.EntityServices.Tables.Core.Implementations;

namespace Azure.EntityServices.Tables.Core.Abstractions
{
    public interface INativeTableBatchClientFactory<T> where T : class, new()
    {
        INativeTableBatchClient<T> Create(
            EntityTableClientOptions options,
            Func<EntityTransactionGroup, Task<EntityTransactionGroup>> preProcessor,
            IEntityAdapter<T> entityAdapter,
            OnTransactionSubmitted onTransactionSubmittedHandler = null
            );
    }
}