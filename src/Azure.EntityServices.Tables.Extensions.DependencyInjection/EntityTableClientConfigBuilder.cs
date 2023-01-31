using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public sealed class EntityTableClientBuilder<T> : IEntityTableClientBuilder<T>
    {
        private EntityTableClientOptions _options = new EntityTableClientOptions();
        private EntityTableClientConfig<T> _config = new EntityTableClientConfig<T>();
        private IServiceProvider _serviceProvider;
        private readonly ServicesByNameBuilder<IEntityObserver<T>> _entityObserverBuilder;

        public EntityTableClientBuilder(IServiceCollection serviceCollection)
        {
            _entityObserverBuilder = serviceCollection.AddByName<IEntityObserver<T>>();
        }

        private Action<EntityTableClientOptions> _optionsAction;
        private Action<EntityTableClientConfig<T>> _configAction;

        public EntityTableClientOptions BindOptions()
        {
            _optionsAction.Invoke(_options);
            return _options;
        }

        public EntityTableClientConfig<T> BindConfig(IServiceProvider provider)
        {
            _serviceProvider = provider;
            _configAction.Invoke(_config);
            return _config;
        }

        public void Build()
        {
            _entityObserverBuilder?.Build();
        }

        public IEntityTableClientBuilder<T> ConfigureEntity(Action<EntityTableClientConfig<T>> configurator)
        {
            _configAction = configurator;
            return this;
        }

        public IEntityTableClientBuilder<T> ConfigureOptions(Action<EntityTableClientOptions> optionsConfigurator)
        {
            _optionsAction = optionsConfigurator;
            return this;
        }

        public IEntityTableClientBuilder<T> RegisterObserver<TImplementation>(string observerName)
            where TImplementation : IEntityObserver<T>
        {
            _entityObserverBuilder.Add<TImplementation>(observerName);

            _config.Observers.TryAdd(observerName, () => _serviceProvider.GetServiceByName<IEntityObserver<T>>(observerName));
            return this;
        }

        public IEntityTableClientBuilder<T> ConfigureConnection(string connectionString)
        {
            throw new NotImplementedException();
        }

        public IEntityTableClientBuilder<T> ConfigureConnection(Uri serviceUri)
        {
            throw new NotImplementedException();
        }

        public IEntityTableClientBuilder<T> ConfigureConnection(Uri serviceUri, TableSharedKeyCredential sharedKeyCredential)
        {
            throw new NotImplementedException();
        }
    }
}