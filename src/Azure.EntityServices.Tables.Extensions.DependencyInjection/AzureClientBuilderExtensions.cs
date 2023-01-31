using Azure.Core.Extensions;
using Azure.Data.Tables;
using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{

    public static class AzureClientBuilderExtensions
    {
        /// <summary>
        /// Registers a <see cref="EntityTableClient"/> instance with the provided <paramref name="connectionString"/>
        /// </summary>
        public static IAzureClientBuilder<EntityTableClient<T>, EntityTableClientOptions> AddEntityTableClientService<T,TBuilder>(this TBuilder builder,
            Action<IEntityTableClientBuilder<T>> configBuilder
            )
            where TBuilder : IAzureClientFactoryBuilder
            where T : class, new()

        { 
           
            return builder.RegisterClientFactory<EntityTableClient<T>, EntityTableClientOptions>(options => new EntityTableClient<T>()
            .Configure(options, new EntityTableClientConfig<T>()));
        }

        /// Registers a <see cref="EntityTableClient"/> instance with the provided <paramref name="connectionString"/>
        /// </summary>
        public static IAzureClientBuilder<EntityTableClient<T>, EntityTableClientOptions> AddEntityTableClientService<T, TBuilder>(this TBuilder builder, string connectionString)
            where TBuilder : IAzureClientFactoryBuilder
            where T : class, new()
        {

            return builder.RegisterClientFactory<EntityTableClient<T>, EntityTableClientOptions>(options => EntityTableClient.Create<T>(connectionString)
          .Configure(options, new EntityTableClientConfig<T>()));
        }

        /// <summary>
        /// Registers a <see cref="EntityTableClient"/> instance with the provided <paramref name="serviceUri"/>
        /// </summary>
        public static IAzureClientBuilder<EntityTableClient<T>, EntityTableClientOptions> AddEntityTableClientService<T, TBuilder>(this TBuilder builder, Uri serviceUri)
            where TBuilder : IAzureClientFactoryBuilderWithCredential
            where T : class, new()
        {
            
            return builder.RegisterClientFactory<EntityTableClient<T>, EntityTableClientOptions>(
                (options, token) => (token != null ? EntityTableClient.Create<T>(serviceUri, token) : EntityTableClient.Create<T>(serviceUri))
                .Configure(options, new EntityTableClientConfig<T>()), requiresCredential: false);
        }

        /// <summary>
        /// Registers a <see cref="EntityTableClient"/> instance with the provided <paramref name="serviceUri"/> and <paramref name="sharedKeyCredential"/>
        /// </summary>
        public static IAzureClientBuilder<EntityTableClient<T>, EntityTableClientOptions> AddEntityTableClientService<T, TBuilder>(this TBuilder builder, Uri serviceUri, TableSharedKeyCredential sharedKeyCredential)
            where TBuilder : IAzureClientFactoryBuilder
            where T : class, new()
        {
            return builder.RegisterClientFactory<EntityTableClient<T>, EntityTableClientOptions>(options => EntityTableClient.Create<T>(serviceUri, sharedKeyCredential)
         .Configure(options, new EntityTableClientConfig<T>()));
        }
    }
}