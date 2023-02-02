using Azure.Core.Extensions;
using Azure.Data.Tables;
using Microsoft.Extensions.Azure;
using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public static class AzureClientFactoryBuilderExtensions
    {
        /// <summary>
        /// Registers a <see cref="EntityTableClient"/> instance with the provided <paramref name="connectionString"/> and <paramref name="entityBuilderAction"/>
        /// </summary>
        public static IAzureClientBuilder<IEntityTableClient<TEntity>, EntityTableClientOptions> AddEntityTableClient<TEntity>(this AzureClientFactoryBuilder builder,
         string connectionString,
         Action<IEntityTableClientBuilder<TEntity>> entityBuilderAction)
         where TEntity : class, new()

        {
            var entityBuilder = new EntityTableClientBuilder<TEntity>();
            entityBuilderAction.Invoke(entityBuilder);
            return builder.AddClient<IEntityTableClient<TEntity>, EntityTableClientOptions>((options, provider) =>
             {
                 var (innerOptions, config) = entityBuilder.Build(provider);
                 return EntityTableClient.Create<TEntity>(connectionString)
                  .Configure(options ?? innerOptions, config);
             });
        }

        /// <summary>
        /// Registers a <see cref="EntityTableClient"/> instance with the provided <paramref name="serviceUri"/> and <paramref name="entityBuilderAction"/>
        /// </summary>
        public static IAzureClientBuilder<IEntityTableClient<TEntity>, EntityTableClientOptions> AddEntityTableClient<TEntity>(this AzureClientFactoryBuilder builder,
            Uri serviceUri,
            Action<IEntityTableClientBuilder<TEntity>> entityBuilderAction)
        where TEntity : class, new()
        {
            var entityBuilder = new EntityTableClientBuilder<TEntity>();
            entityBuilderAction.Invoke(entityBuilder);
            return builder.AddClient<IEntityTableClient<TEntity>, EntityTableClientOptions>((options, token, provider) =>
            {
                var (innerOptions, config) = entityBuilder.Build(provider);
                return EntityTableClient.Create<TEntity>(serviceUri, token)
                 .Configure(options ?? innerOptions, config);
            });
        }

        /// <summary>
        /// Registers a <see cref="EntityTableClient"/> instance with the provided <paramref name="serviceUri"/>, <paramref name="sharedKeyCredential"/> and <paramref name="entityBuilderAction"/>
        /// </summary>
        public static IAzureClientBuilder<IEntityTableClient<TEntity>, EntityTableClientOptions> AddEntityTableClient<TEntity>(this AzureClientFactoryBuilder builder,
            Uri serviceUri,
            TableSharedKeyCredential sharedKeyCredential,
            Action<IEntityTableClientBuilder<TEntity>> entityBuilderAction)
              where TEntity : class, new()
        {
            var entityBuilder = new EntityTableClientBuilder<TEntity>();
            entityBuilderAction.Invoke(entityBuilder);
            return builder.AddClient<IEntityTableClient<TEntity>, EntityTableClientOptions>((options, provider) =>
            {
                var (innerOptions,config) = entityBuilder.Build(provider);
                return EntityTableClient.Create<TEntity>(serviceUri, sharedKeyCredential)
                 .Configure(options ?? innerOptions, config);
            });
        }
    }
}