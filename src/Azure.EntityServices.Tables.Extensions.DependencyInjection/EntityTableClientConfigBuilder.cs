using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public class EntityTableClientConfigBuilder<T>: EntityTableClientConfig<T>
    {
        internal EntityTableClientConfigBuilder(IServiceProvider serviceProvider,ServicesByNameBuilder<IEntityObserver<T>> entityObserverBuilder)
        {
            ServiceProvider = serviceProvider;
            EntityObserverBuilder = entityObserverBuilder;

        }

        public IServiceProvider ServiceProvider { get; }
        public ServicesByNameBuilder<IEntityObserver<T>> EntityObserverBuilder { get; }

        public void Build()
        {
            EntityObserverBuilder?.Build();
        }
    }
}