using Azure.Data.Tables;
using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public sealed class EntityTableClientBuilder<T> : IEntityTableClientBuilder<T>
        where T : class, new()
    {
        public EntityTableClientBuilder()
        {
        }

        private Action<EntityTableClientOptions> _optionsAction;
        private Action<IServiceProvider, EntityTableClientConfig<T>> _configAction;

        public (EntityTableClientOptions, EntityTableClientConfig<T>) Build(IServiceProvider serviceProvider)
        {
            var sp = serviceProvider;
            var options = new EntityTableClientOptions();
            var config = new EntityTableClientConfig<T>();
            _optionsAction?.Invoke(options);
            _configAction?.Invoke(sp, config);
            return (options, config);
        }

        public IEntityTableClientBuilder<T> ConfigureEntity(Action<EntityTableClientConfig<T>> configurator)
        {
            _configAction = (_, config) => configurator.Invoke(config);
            return this;
        }

        public IEntityTableClientBuilder<T> ConfigureEntity(Action<IServiceProvider, EntityTableClientConfig<T>> configurator)
        {
            _configAction = configurator;
            return this;
        }

        public IEntityTableClientBuilder<T> ConfigureOptions(Action<EntityTableClientOptions> optionsConfigurator)
        {
            _optionsAction = optionsConfigurator;
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