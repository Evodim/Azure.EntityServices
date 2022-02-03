using System;

namespace Azure.EntityServices.Tables
{
    public interface IEntityObserver<T> : IObserver<IEntityOperationContext<T>>
    {
    }
}