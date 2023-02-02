using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public interface IEntityTableClientBuilder<TEntity>
    {
        IEntityTableClientBuilder<TEntity> ConfigureEntity(Action<EntityTableClientConfig<TEntity>> configureEntity);

        IEntityTableClientBuilder<TEntity> ConfigureEntity(Action<IServiceProvider, EntityTableClientConfig<TEntity>> configureEntity);

        IEntityTableClientBuilder<TEntity> ConfigureOptions(Action<EntityTableClientOptions> configureOptions);

        (EntityTableClientOptions, EntityTableClientConfig<TEntity>) Build(IServiceProvider serviceProvider);
    }
}