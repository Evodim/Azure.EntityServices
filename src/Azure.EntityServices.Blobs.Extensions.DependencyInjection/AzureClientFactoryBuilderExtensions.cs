using Azure.Core.Extensions;
using Azure.EntityServices.Blobs;
using Azure.Storage;
using Microsoft.Extensions.Azure;
using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public static class AzureClientFactoryBuilderExtensions
    {
        /// <summary>
        /// Registers a <see cref="EntityBlobClient"/> instance with the provided <paramref name="connectionString"/> and <paramref name="entityBuilderAction"/>
        /// </summary>
        public static IAzureClientBuilder<IEntityBlobClient<TEntity>, EntityBlobClientOptions> AddEntityBlobClient<TEntity>(this AzureClientFactoryBuilder builder,
         string connectionString,
         Action<IEntityBlobClientBuilder<TEntity>> entityBuilderAction)
         where TEntity : class, new()

        {
            var entityBuilder = new EntityBlobClientBuilder<TEntity>();
            entityBuilderAction.Invoke(entityBuilder);
            return builder.AddClient<IEntityBlobClient<TEntity>, EntityBlobClientOptions>((options, provider) =>
             {
                 var (innerOptions, config) = entityBuilder.Build(provider);
                 return EntityBlobClient.Create<TEntity>(connectionString)
                  .Configure(innerOptions ?? options, config);
             });
        }

        /// <summary>
        /// Registers a <see cref="EntityBlobClient"/> instance with the provided <paramref name="serviceUri"/> and <paramref name="entityBuilderAction"/>
        /// </summary>
        public static IAzureClientBuilder<IEntityBlobClient<TEntity>, EntityBlobClientOptions> AddEntityBlobClient<TEntity>(this AzureClientFactoryBuilder builder,
            Uri serviceUri,
            Action<IEntityBlobClientBuilder<TEntity>> entityBuilderAction)
        where TEntity : class, new()
        {
            var entityBuilder = new EntityBlobClientBuilder<TEntity>();
            entityBuilderAction.Invoke(entityBuilder);
            return builder.AddClient<IEntityBlobClient<TEntity>, EntityBlobClientOptions>((options, token, provider) =>
            {
                var (innerOptions, config) = entityBuilder.Build(provider);
                return EntityBlobClient.Create<TEntity>(serviceUri, token)
                 .Configure(innerOptions ?? options, config);
            });
        }

        /// <summary>
        /// Registers a <see cref="EntityBlobClient"/> instance with the provided <paramref name="serviceUri"/>, <paramref name="sharedKeyCredential"/> and <paramref name="entityBuilderAction"/>
        /// </summary>
        public static IAzureClientBuilder<IEntityBlobClient<TEntity>, EntityBlobClientOptions> AddEntityBlobClient<TEntity>(this AzureClientFactoryBuilder builder,
            Uri serviceUri,
            StorageSharedKeyCredential sharedKeyCredential,
            Action<IEntityBlobClientBuilder<TEntity>> entityBuilderAction)
              where TEntity : class, new()
        {
            var entityBuilder = new EntityBlobClientBuilder<TEntity>();
            entityBuilderAction.Invoke(entityBuilder);
            return builder.AddClient<IEntityBlobClient<TEntity>, EntityBlobClientOptions>((options, provider) =>
            {
                var (innerOptions, config) = entityBuilder.Build(provider);
                return EntityBlobClient.Create<TEntity>(serviceUri, sharedKeyCredential)
                 .Configure(innerOptions ?? options, config);
            });
        }
    }
}