namespace Azure.EntityServices.Tables
{
    public interface IEntityTableClientRuntimeConfig<T>
    {
        void AddObserver(string name, IEntityObserver<T> observer);

        void RemoveObserver(string name);
    }
}