using Azure.Data.Tables;
using Azure.EntityServices.Tables.Extensions.DependencyInjection;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace Azure.EntityServices.Tables.Extensions
{
    public static class EntityTableServicesCollectionExtensions
    {
        public static IServiceCollection AddEntityTableClient<T>(this IServiceCollection services, string connectionString, Action<IEntityTableClientBuilder<T>> configBuilder)
        where T : class, new()
        {
            return services
                .AddEntityTableClient(configBuilder)
                .WithTableClientService<T>(connectionString);
        }

        public static IServiceCollection AddEntityTableClient<T>(this IServiceCollection services, Uri serviceUri, Action<IEntityTableClientBuilder<T>> configBuilder)
        where T : class, new()
        {
            return services
                .AddEntityTableClient(configBuilder)
                .WithTableClientService<T>(serviceUri);
        }

        public static IServiceCollection AddEntityTableClient<T>(this IServiceCollection services, Uri serviceUri, TableSharedKeyCredential sharedKeyCredential, Action<IEntityTableClientBuilder<T>> configBuilder)
        where T : class, new()
        {
            return services
                .AddEntityTableClient(configBuilder)
                .WithTableClientService<T>(serviceUri, sharedKeyCredential);
        }

        private static IServiceCollection AddEntityTableClient<T>(this IServiceCollection services,
        Action<IEntityTableClientBuilder<T>> configBuilder
        )
        where T : class, new()
        {
            var builder = new EntityTableClientBuilder<T>();

            configBuilder.Invoke(builder);

            services.AddTransient<IEntityTableClient<T>>(sp =>
            {
                var (options, config) = builder.Build(sp);
                var tableServiceFactory = sp.GetRequiredService<IAzureClientFactory<TableServiceClient>>();

                return EntityTableClient
                .Create<T>(tableServiceFactory)
                .Configure(options, config);
            });
            return services;
        }

        public static IServiceCollection WithTableClientService<T>(this IServiceCollection services,
         string connectionString,
         Action<TableClientOptions> optionsAction = null)
         where T : class, new()

        {
            services.AddAzureClients(clientBuilder =>
            {
                clientBuilder
                 .AddTableServiceClient(connectionString)
                 .ConfigureOptions(options => optionsAction?.Invoke(options))
                 .WithName(typeof(T).Name);
            });
            return services;
        }

        public static IServiceCollection WithTableClientService<T>(this IServiceCollection services,
        Uri endPoint,
        Action<TableClientOptions> optionsAction = null)
        where T : class, new()

        {
            services.AddAzureClients(clientBuilder =>
            {
                clientBuilder
                 .AddTableServiceClient(endPoint)
                 .ConfigureOptions(options => optionsAction?.Invoke(options))
                 .WithName(typeof(T).Name);
            });
            return services;
        }

        public static IServiceCollection WithTableClientService<T>(this IServiceCollection services,
         Uri endPoint,
         TableSharedKeyCredential tableSharedKeyCredential,
         Action<TableClientOptions> optionsAction = null)
         where T : class, new()

        {
            services.AddAzureClients(clientBuilder =>
            {
                clientBuilder
                 .AddTableServiceClient(endPoint, tableSharedKeyCredential)
                 .ConfigureOptions(options => optionsAction?.Invoke(options))
                 .WithName(typeof(T).Name);
            });
            return services;
        }
    }
}