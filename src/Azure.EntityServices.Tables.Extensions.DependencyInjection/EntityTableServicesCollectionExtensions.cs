using Azure.Data.Tables;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public static class EntityTableServicesCollectionExtensions
    {
        public static IServiceCollection AddEntityTableClient<TEntity>(this IServiceCollection services, string connectionString, Action<IEntityTableClientBuilder<TEntity>> configBuilder)
        where TEntity : class, new()
        {
            return services
                .AddEntityTableClient(configBuilder)
                .WithTableClientService<TEntity>(connectionString);
        }

        public static IServiceCollection AddEntityTableClient<TEntity>(this IServiceCollection services, Uri serviceUri, Action<IEntityTableClientBuilder<TEntity>> configBuilder)
        where TEntity : class, new()
        {
            return services
                .AddEntityTableClient(configBuilder)
                .WithTableClientService<TEntity>(serviceUri);
        }

        public static IServiceCollection AddEntityTableClient<TEntity>(this IServiceCollection services, Uri serviceUri, TableSharedKeyCredential sharedKeyCredential, Action<IEntityTableClientBuilder<TEntity>> configBuilder)
        where TEntity : class, new()
        {
            return services
                .AddEntityTableClient(configBuilder)
                .WithTableClientService<TEntity>(serviceUri, sharedKeyCredential);
        }

        private static IServiceCollection AddEntityTableClient<TEntity>(this IServiceCollection services,
        Action<IEntityTableClientBuilder<TEntity>> configBuilder
        )
        where TEntity : class, new()
        {
            var builder = new EntityTableClientBuilder<TEntity>();

            configBuilder.Invoke(builder);

            services.AddTransient<IEntityTableClient<TEntity>>(sp =>
            {
                var (options, config) = builder.Build(sp);
                var tableServiceFactory = sp.GetRequiredService<IAzureClientFactory<TableServiceClient>>();

                return EntityTableClient
                .Create<TEntity>(tableServiceFactory.CreateClient(typeof(TEntity).Name))
                .Configure(options, config);
            });
            return services;
        }

        internal static IServiceCollection WithTableClientService<TEntity>(this IServiceCollection services,
         string connectionString,
         Action<TableClientOptions> optionsAction = null)
         where TEntity : class, new()

        {
            services.AddAzureClients(clientBuilder =>
            {
                clientBuilder
                 .AddTableServiceClient(connectionString)
                 .ConfigureOptions(options => optionsAction?.Invoke(options))
                 .WithName(typeof(TEntity).Name);
            });
            return services;
        }

        internal static IServiceCollection WithTableClientService<TEntity>(this IServiceCollection services,
        Uri endPoint,
        Action<TableClientOptions> optionsAction = null)
        where TEntity : class, new()

        {
            services.AddAzureClients(clientBuilder =>
            {
                clientBuilder
                 .AddTableServiceClient(endPoint)
                 .ConfigureOptions(options => optionsAction?.Invoke(options))
                 .WithName(typeof(TEntity).Name);
            });
            return services;
        }

        internal static IServiceCollection WithTableClientService<TEntity>(this IServiceCollection services,
         Uri endPoint,
         TableSharedKeyCredential tableSharedKeyCredential,
         Action<TableClientOptions> optionsAction = null)
         where TEntity : class, new()

        {
            services.AddAzureClients(clientBuilder =>
            {
                clientBuilder
                 .AddTableServiceClient(endPoint, tableSharedKeyCredential)
                 .ConfigureOptions(options => optionsAction?.Invoke(options))
                 .WithName(typeof(TEntity).Name);
            });
            return services;
        }
    }
}