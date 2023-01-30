using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public static class EntityTableClientBuilderExtensions
    {

        public static IEntityTableClientBuilder<T> RegisterObserver<T, TImplementation>(this IEntityTableClientBuilder<T> builder, string observerName)
            where TImplementation : IEntityObserver<T>
        {
           
            (builder as EntityTableClientBuilder<T>).EntityObserverBuilder.Add<TImplementation>(observerName);
            
            builder.Config.Observers.TryAdd(observerName, () => (builder as EntityTableClientBuilder<T>).ServiceProvider.GetServiceByName<IEntityObserver<T>>(observerName));
            return builder;
        }
    }
}