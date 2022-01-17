using System;

namespace Azure.EntityServices.Table
{
    public interface IEntityObserver<T> : IObserver<IEntityOperationContext<T>>
    {
    }
}