using Azure.EntityServices.Blobs;
using Azure.EntityServices.Tables.Extensions.DependencyInjection;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace Azure.EntityServices.Tables.Extensions
{
    public static class EntityBlobServicesCollectionExtensions
    {
        public static IServiceCollection AddEntityBlobClient<TEntity>(this IServiceCollection services, string connectionString, Action<IEntityBlobClientBuilder<TEntity>> configBuilder)
        where TEntity : class, new()
        {
            return services
                .AddEntityBlobClient(configBuilder)
                .WithTableClientService<TEntity>(connectionString);
        }

        public static IServiceCollection AddEntityBlobClient<TEntity>(this IServiceCollection services, Uri serviceUri, Action<IEntityBlobClientBuilder<TEntity>> configBuilder)
        where TEntity : class, new()
        {
            return services
                .AddEntityBlobClient(configBuilder)
                .WithTableClientService<TEntity>(serviceUri);
        }

        public static IServiceCollection AddEntityBlobClient<TEntity>(this IServiceCollection services, Uri serviceUri, StorageSharedKeyCredential sharedKeyCredential, Action<IEntityBlobClientBuilder<TEntity>> configBuilder)
        where TEntity : class, new()
        {
            return services
                .AddEntityBlobClient(configBuilder)
                .WithTableClientService<TEntity>(serviceUri, sharedKeyCredential);
        }

        private static IServiceCollection AddEntityBlobClient<TEntity>(this IServiceCollection services,
        Action<IEntityBlobClientBuilder<TEntity>> configBuilder
        )
        where TEntity : class, new()
        {
            var builder = new EntityBlobClientBuilder<TEntity>();

            configBuilder.Invoke(builder);

            services.AddTransient<IEntityBlobClient<TEntity>>(sp =>
            {
                var (options, config) = builder.Build(sp);
                var blobServiceFactory = sp.GetRequiredService<IAzureClientFactory<BlobServiceClient>>();

                return EntityBlobClient
                .Create<TEntity>(new BlobService(blobServiceFactory.CreateClient(typeof(TEntity).Name)))
                .Configure(options, config);
            });
            return services;
        }

        internal static IServiceCollection WithTableClientService<TEntity>(this IServiceCollection services,
         string connectionString,
         Action<BlobClientOptions> optionsAction = null)
         where TEntity : class, new()

        {
            services.AddAzureClients(clientBuilder =>
            {
                clientBuilder
                 .AddBlobServiceClient(connectionString)
                 .ConfigureOptions(options => optionsAction?.Invoke(options))
                 .WithName(typeof(TEntity).Name);
            });
            return services;
        }

        internal static IServiceCollection WithTableClientService<TEntity>(this IServiceCollection services,
        Uri endPoint,
        Action<BlobClientOptions> optionsAction = null)
        where TEntity : class, new()

        {
            services.AddAzureClients(clientBuilder =>
            {
                clientBuilder
                 .AddBlobServiceClient(endPoint)
                 .ConfigureOptions(options => optionsAction?.Invoke(options))
                 .WithName(typeof(TEntity).Name);
            });
            return services;
        }

        internal static IServiceCollection WithTableClientService<TEntity>(this IServiceCollection services,
         Uri endPoint,
         StorageSharedKeyCredential tableSharedKeyCredential,
         Action<BlobClientOptions> optionsAction = null)
         where TEntity : class, new()

        {
            services.AddAzureClients(clientBuilder =>
            {
                clientBuilder
                 .AddBlobServiceClient(endPoint, tableSharedKeyCredential)
                 .ConfigureOptions(options => optionsAction?.Invoke(options))
                 .WithName(typeof(TEntity).Name);
            });
            return services;
        }
    }
}