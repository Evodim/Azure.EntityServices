

using Azure.Core;
using System;
using System.Dynamic;

namespace Azure.EntityServices.Blobs
{
    public static class EntityBlobClient
    {

        public static EntityBlobClient<T> Create<T>(BlobStorageService blobStorageService)
           
             where T : class, new()
        {
            _ = blobStorageService ?? throw new ArgumentNullException(nameof(blobStorageService));

            return new EntityBlobClient<T>(blobStorageService);
        }
        public static EntityBlobClient<T> Create<T>(string connectionString, BlobStorageServiceOptions blobStorageServiceOptions = null)
       where T : class, new()
        { 
            return new EntityBlobClient<T>(new BlobStorageService(blobStorageServiceOptions));
        }

        //public static EntityBlobClient<T> Create<T>(string connectionString, TableClientOptions tableClientOptions = null)
        // where T : class, new()
        //{
        //    return new EntityBlobClient<T>(new TableServiceClient(connectionString, tableClientOptions));
        //}

        //public static EntityBlobClient<T> Create<T>(Uri endPoint, AzureSasCredential azureSasCredential, TableClientOptions tableClientOptions = null)
        // where T : class, new()
        //{
        //    return new EntityBlobClient<T>(new TableServiceClient(endPoint, azureSasCredential, tableClientOptions));
        //}

        //public static EntityBlobClient<T> Create<T>(Uri endPoint, TableClientOptions tableClientOptions = null)
        // where T : class, new()
        //{
        //    return new EntityBlobClient<T>(new TableServiceClient(endPoint, tableClientOptions));
        //}

        //public static EntityBlobClient<T> Create<T>(Uri endPoint, TableSharedKeyCredential sharedKeyCredential, TableClientOptions tableClientOptions = null)
        //where T : class, new()
        //{
        //    return new EntityBlobClient<T>(new TableServiceClient(endPoint, sharedKeyCredential, tableClientOptions));
        //}

        //public static EntityBlobClient<T> Create<T>(Uri endPoint, TokenCredential tokenCredential, TableClientOptions tableClientOptions = null)
        //where T : class, new()
        //{
        //    return new EntityBlobClient<T>(new TableServiceClient(endPoint, tokenCredential, tableClientOptions));
        //}
    }
}
