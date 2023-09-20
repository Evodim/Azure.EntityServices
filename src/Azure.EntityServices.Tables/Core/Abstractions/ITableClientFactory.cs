using System.Threading.Tasks;
using System;
using Azure.EntityServices.Tables.Core.Implementations;
using System.Collections.Generic;

namespace Azure.EntityServices.Tables.Core.Abstractions
{
    public interface ITableClientFactory<T> where T : class, new()
    {
        ITableClientFacade<T> Create(
            EntityTableClientConfig<T> config,
            EntityTableClientOptions options,
            Func<EntityTransactionGroup, Task<EntityTransactionGroup>> preProcessor,
            IEntityAdapter<T> entityAdapter,
            Func<IEnumerable<EntityOperation>, Task> onTransactionSubmittedHandler = null
            );
    }
}