using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Core.Abstractions
{
    public interface ITableClientFactory
    {
        ITableClientFacade<T> Create<T>(
            EntityTableClientConfig<T> config,
            EntityTableClientOptions options,
            Func<EntityTransactionGroup, Task<EntityTransactionGroup>> preProcessor,
            IEntityAdapter<T> entityAdapter,
            Func<IEnumerable<EntityOperation>, Task> onTransactionSubmittedHandler = null
            ) where T : class, new();
    }
}