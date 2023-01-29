using System;

namespace Azure.EntityServices.Tables
{
    public interface IEntityTableClientRuntimeConfig<T>
    {
        void AddObserver(string name, Func<IEntityObserver<T>> observer);

        void RemoveObserver(string name);
    }
}