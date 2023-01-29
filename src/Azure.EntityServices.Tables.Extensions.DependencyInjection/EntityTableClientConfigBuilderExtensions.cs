namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public static partial class EntityTableClientConfigBuilderExtensions
    {
        public static EntityTableClientConfig<T> AddObserver<T, TImplementation>(this EntityTableClientConfigBuilder<T> builder, string observerName)
            where TImplementation : IEntityObserver<T>
        {
            builder.EntityObserverBuilder.Add<TImplementation>(observerName);

            builder.Observers.TryAdd(observerName, ()=> builder.ServiceProvider.GetServiceByName<IEntityObserver<T>>(observerName));
            return builder;
        }
    }
}