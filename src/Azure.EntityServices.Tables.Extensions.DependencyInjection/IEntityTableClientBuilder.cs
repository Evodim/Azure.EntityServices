using Azure.Data.Tables;
using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{ 
    public interface IEntityTableClientBuilder<T>
    {
        IEntityTableClientBuilder<T> ConfigureConnection(string connectionString);
        IEntityTableClientBuilder<T> ConfigureConnection(Uri serviceUri);
        IEntityTableClientBuilder<T> ConfigureConnection(Uri serviceUri, TableSharedKeyCredential sharedKeyCredential); 
        IEntityTableClientBuilder<T> ConfigureEntity(Action<EntityTableClientConfig<T>> configurator); 
        IEntityTableClientBuilder<T> ConfigureOptions(Action<EntityTableClientOptions> optionsConfigurator);

        void Build();
    }
}