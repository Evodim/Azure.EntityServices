using Azure.Core;
using Azure.Data.Tables;
using Microsoft.Extensions.Azure;
using System;

namespace Azure.EntityServices.Tables
{
    public static class EntityTableClient
    {
        public static EntityTableClient<T> Create<T>(Action<EntityTableClientOptions> optionsDelegate, Action<EntityTableClientConfig<T>> configurator)
            where T : class, new()
        {
            _ = optionsDelegate ?? throw new ArgumentNullException(nameof(optionsDelegate));
            _ = configurator ?? throw new ArgumentNullException(nameof(configurator));

            var options = new EntityTableClientOptions();
            var configuration = new EntityTableClientConfig<T>();

            optionsDelegate.Invoke(options);
            configurator.Invoke(configuration);

            return new EntityTableClient<T>().Configure(options, configuration);
        }

        public static EntityTableClient<T> Create<T>(EntityTableClientOptions options, Action<EntityTableClientConfig<T>> configurator)
          where T : class, new()
        {
            _ = options ?? throw new ArgumentNullException(nameof(options));
            _ = configurator ?? throw new ArgumentNullException(nameof(configurator));

            var configuration = new EntityTableClientConfig<T>();

            configurator.Invoke(configuration);

            return new EntityTableClient<T>().Configure(options, configuration);
        }

        public static EntityTableClient<T> Create<T>(EntityTableClientOptions options, EntityTableClientConfig<T> configuration)
        where T : class, new()
        {
            _ = options ?? throw new ArgumentNullException(nameof(options));
            _ = configuration ?? throw new ArgumentNullException(nameof(configuration));

            return new EntityTableClient<T>().Configure(options, configuration);
        }

        public static EntityTableClient<T> Create<T>(TableServiceClient tableServiceClient)
           where T : class, new()
        {
            _ = tableServiceClient ?? throw new ArgumentNullException(nameof(tableServiceClient));

            return new EntityTableClient<T>(tableServiceClient);
        }
        public static EntityTableClient<T> Create<T>(IAzureClientFactory<TableServiceClient> tableServiceClientFactory)
           where T : class, new()
        {
            _ = tableServiceClientFactory ?? throw new ArgumentNullException(nameof(tableServiceClientFactory));

            return new EntityTableClient<T>(tableServiceClientFactory);
        }

        public static EntityTableClient<T> Create<T>(string connectionString, TableClientOptions tableClientOptions = null)
         where T : class, new()
        {
            return new EntityTableClient<T>(new TableServiceClient(connectionString, tableClientOptions));
        }

        public static EntityTableClient<T> Create<T>(Uri endPoint, AzureSasCredential azureSasCredential, TableClientOptions tableClientOptions = null)
         where T : class, new()
        {
            return new EntityTableClient<T>(new TableServiceClient(endPoint, azureSasCredential, tableClientOptions));
        }

        public static EntityTableClient<T> Create<T>(Uri endPoint, TableClientOptions tableClientOptions = null)
         where T : class, new()
        {
            return new EntityTableClient<T>(new TableServiceClient(endPoint, tableClientOptions));
        }

        public static EntityTableClient<T> Create<T>(Uri endPoint, TableSharedKeyCredential sharedKeyCredential, TableClientOptions tableClientOptions = null)
        where T : class, new()
        {
            return new EntityTableClient<T>(new TableServiceClient(endPoint, sharedKeyCredential, tableClientOptions));
        }

        public static EntityTableClient<T> Create<T>(Uri endPoint, TokenCredential tokenCredential, TableClientOptions tableClientOptions = null)
        where T : class, new()
        {
            return new EntityTableClient<T>(new TableServiceClient(endPoint, tokenCredential, tableClientOptions));
        }
    }
}