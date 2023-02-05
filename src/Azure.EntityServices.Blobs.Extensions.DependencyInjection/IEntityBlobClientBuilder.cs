using Azure.EntityServices.Blobs;
using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public interface IEntityBlobClientBuilder<TEntity>
    {
        IEntityBlobClientBuilder<TEntity> ConfigureEntity(Action<EntityBlobClientConfig<TEntity>> configureEntity);

        IEntityBlobClientBuilder<TEntity> ConfigureEntity(Action<IServiceProvider, EntityBlobClientConfig<TEntity>> configureEntity);

        IEntityBlobClientBuilder<TEntity> ConfigureOptions(Action<EntityBlobClientOptions> configureOptions);

        (EntityBlobClientOptions, EntityBlobClientConfig<TEntity>) Build(IServiceProvider serviceProvider);
    }
}