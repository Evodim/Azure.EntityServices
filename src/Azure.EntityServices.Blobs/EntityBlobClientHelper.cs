using Azure.Core;
using Azure.Storage;
using Azure.Storage.Blobs;
using System;

namespace Azure.EntityServices.Blobs
{
    public static class EntityBlobClient
    {
        public static EntityBlobClient<T> Create<T>(BlobService blobStorageService)

             where T : class, new()
        {
            _ = blobStorageService ?? throw new ArgumentNullException(nameof(blobStorageService));

            return new EntityBlobClient<T>(blobStorageService);
        }

        public static EntityBlobClient<T> Create<T>(string connectionString, BlobClientOptions blobClientOptions = null)
        where T : class, new()
        {
            return new EntityBlobClient<T>(new BlobService(new BlobServiceClient(connectionString, blobClientOptions)));
        }

        public static EntityBlobClient<T> Create<T>(Uri endPoint, AzureSasCredential azureSasCredential, BlobClientOptions blobClientOptions = null)
        where T : class, new()
        {
            return new EntityBlobClient<T>(new BlobService(new BlobServiceClient(endPoint, azureSasCredential, blobClientOptions)));
        }

        public static EntityBlobClient<T> Create<T>(Uri endPoint, BlobClientOptions blobClientOptions = null)
        where T : class, new()
        {
            return new EntityBlobClient<T>(new BlobService(new BlobServiceClient(endPoint, blobClientOptions)));
        }

        public static EntityBlobClient<T> Create<T>(Uri endPoint, StorageSharedKeyCredential sharedKeyCredential, BlobClientOptions blobClientOptions = null)
        where T : class, new()
        {
            return new EntityBlobClient<T>(new BlobService(new BlobServiceClient(endPoint, sharedKeyCredential, blobClientOptions)));
        }

        public static EntityBlobClient<T> Create<T>(Uri endPoint, TokenCredential tokenCredential, BlobClientOptions blobClientOptions = null)
        where T : class, new()
        {
            return new EntityBlobClient<T>(new BlobService(new BlobServiceClient(endPoint, tokenCredential, blobClientOptions)));
        }
    }
}