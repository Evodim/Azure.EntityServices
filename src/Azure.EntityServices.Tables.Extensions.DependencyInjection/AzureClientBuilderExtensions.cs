using Azure.Core.Extensions;
using Azure.Data.Tables;
using Microsoft.Extensions.Azure;
using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public static class AzureClientBuilderExtensions
    {
        /// Registers a <see cref="EntityTableClient"/> instance with the provided <paramref name="connectionString"/>
        public static IAzureClientBuilder<IEntityTableClient<TEntity>, EntityTableClientOptions> AddEntityTableClient<TEntity>(this AzureClientFactoryBuilder builder,
         string connectionString,
         Action<IEntityTableClientBuilder<TEntity>> entityBuilderAction)
         where TEntity : class, new()

        {
            var entityBuilder = new EntityTableClientBuilder<TEntity>();
            entityBuilderAction.Invoke(entityBuilder);
            return builder.AddClient<IEntityTableClient<TEntity>, EntityTableClientOptions>((options, provider) =>
             {
                 var config = entityBuilder.Build(provider);
                 return EntityTableClient.Create<TEntity>(new TableServiceClient(connectionString))
                  .Configure(options, config.Item2);
             });
        }

        /// <summary>
        /// Registers a <see cref="EntityTableClient"/> instance with the provided <paramref name="serviceUri"/>/>
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
                var config = entityBuilder.Build(provider);
                return EntityTableClient.Create<TEntity>(serviceUri, token)
                 .Configure(options, config.Item2);
            });
        }

        /// <summary>
        /// Registers a <see cref="EntityTableClient"/> instance with the provided <paramref name="serviceUri"/> and <paramref name="sharedKeyCredential"/>
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
                var config = entityBuilder.Build(provider);
                return EntityTableClient.Create<TEntity>(serviceUri, sharedKeyCredential)
                 .Configure(options, config.Item2);
            });
        }
    }
}