namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public interface IServiceBag<in K, out T>
    {
        T Get(K key);
    }
}