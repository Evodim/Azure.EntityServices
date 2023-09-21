using System;

namespace Azure.EntityServices.Tables.Core.Abstractions
{
    public interface IObservableEntityTableClient<T>
    {
        void AddObserver(string name, Func<IEntityObserver<T>> observer);
        void RemoveObserver(string name);
    }
}