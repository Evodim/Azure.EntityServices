using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public sealed class EntityTableClientBuilder<TEntity> : IEntityTableClientBuilder<TEntity>
        where TEntity : class, new()
    {
        public EntityTableClientBuilder()
        {
        }

        private Action<EntityTableClientOptions> _optionsAction;
        private Action<IServiceProvider, EntityTableClientConfig<TEntity>> _configAction;

        public (EntityTableClientOptions, EntityTableClientConfig<TEntity>) Build(IServiceProvider serviceProvider)
        {
            var sp = serviceProvider;
            var options = new EntityTableClientOptions();
            var config = new EntityTableClientConfig<TEntity>();
            _optionsAction?.Invoke(options);
            _configAction?.Invoke(sp,config);
            return (options, config);
        }

        public IEntityTableClientBuilder<TEntity> ConfigureEntity(Action<EntityTableClientConfig<TEntity>> configureEntity)
        {
            _configAction = (_, config) => configureEntity.Invoke(config);
            return this;
        }

        public IEntityTableClientBuilder<TEntity> ConfigureEntity(Action<IServiceProvider, EntityTableClientConfig<TEntity>> configureEntity)
        {
            _configAction = configureEntity;
            return this;
        }

        public IEntityTableClientBuilder<TEntity> ConfigureOptions(Action<EntityTableClientOptions> configureOptions)
        {
            _optionsAction = configureOptions;
            return this;
        }
    }
}