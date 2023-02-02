using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public interface IEntityTableClientBuilder<TEntity>
    {
        IEntityTableClientBuilder<TEntity> ConfigureEntity(Action<EntityTableClientConfig<TEntity>> entityConfigurator);

        IEntityTableClientBuilder<TEntity> ConfigureEntity(Action<IServiceProvider, EntityTableClientConfig<TEntity>> entityConfigurator);

        IEntityTableClientBuilder<TEntity> ConfigureOptions(Action<EntityTableClientOptions> optionsConfigurator);

        (EntityTableClientOptions, EntityTableClientConfig<TEntity>) Build(IServiceProvider serviceProvider);
    }
}