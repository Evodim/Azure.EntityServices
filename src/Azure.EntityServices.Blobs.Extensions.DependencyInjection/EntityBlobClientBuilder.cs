using Azure.EntityServices.Blobs;
using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public sealed class EntityBlobClientBuilder<TEntity> : IEntityBlobClientBuilder<TEntity>
        where TEntity : class, new()
    {
        public EntityBlobClientBuilder()
        {
        }

        private Action<EntityBlobClientOptions> _optionsAction;
        private Action<IServiceProvider, EntityBlobClientConfig<TEntity>> _configAction;

        public (EntityBlobClientOptions, EntityBlobClientConfig<TEntity>) Build(IServiceProvider serviceProvider)
        {
            var sp = serviceProvider;
            var options = new EntityBlobClientOptions();
            var config = new EntityBlobClientConfig<TEntity>();
            _optionsAction?.Invoke(options);
            _configAction?.Invoke(sp, config);
            return (options, config);
        }

        public IEntityBlobClientBuilder<TEntity> ConfigureEntity(Action<EntityBlobClientConfig<TEntity>> configureEntity)
        {
            _configAction = (_, config) => configureEntity.Invoke(config);
            return this;
        }

        public IEntityBlobClientBuilder<TEntity> ConfigureEntity(Action<IServiceProvider, EntityBlobClientConfig<TEntity>> configureEntity)
        {
            _configAction = configureEntity;
            return this;
        }

        public IEntityBlobClientBuilder<TEntity> ConfigureOptions(Action<EntityBlobClientOptions> configureOptions)
        {
            _optionsAction = configureOptions;
            return this;
        }
    }
}