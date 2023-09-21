using Azure.EntityServices.Tables.Core.Abstractions;
using System.Collections.Generic;

namespace Azure.EntityServices.Tables
{
    public interface IEntityObserver<T> : IAsyncObserver<IEnumerable<EntityOperationContext<T>>>
    {
        string Name { get; }
    }
}