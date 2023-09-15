using Azure.Core;
using Azure.Data.Tables;
using System;

namespace Azure.EntityServices.Tables
{
    public static class EntityTableClient
    {
        public static EntityTableClient<T> Create<T>(TableServiceClient tableServiceClient)
           where T : class, new()
        {
            _ = tableServiceClient ?? throw new ArgumentNullException(nameof(tableServiceClient));

            return new EntityTableClient<T>(tableServiceClient);
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