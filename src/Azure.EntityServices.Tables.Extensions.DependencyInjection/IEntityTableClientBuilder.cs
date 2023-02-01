using Azure.Data.Tables;
using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{ 
    public interface IEntityTableClientBuilder<TEntity>
    {
        IEntityTableClientBuilder<TEntity> ConfigureConnection(string connectionString);
        IEntityTableClientBuilder<TEntity> ConfigureConnection(Uri serviceUri);
        IEntityTableClientBuilder<TEntity> ConfigureConnection(Uri serviceUri, TableSharedKeyCredential sharedKeyCredential); 
        IEntityTableClientBuilder<TEntity> ConfigureEntity(Action<EntityTableClientConfig<TEntity>> configurator);
        IEntityTableClientBuilder<TEntity> ConfigureEntity(Action<IServiceProvider, EntityTableClientConfig<TEntity>> configurator);
        IEntityTableClientBuilder<TEntity> ConfigureOptions(Action<EntityTableClientOptions> optionsConfigurator);
        (EntityTableClientOptions, EntityTableClientConfig<TEntity>) Build(IServiceProvider serviceProvider);
    }
}