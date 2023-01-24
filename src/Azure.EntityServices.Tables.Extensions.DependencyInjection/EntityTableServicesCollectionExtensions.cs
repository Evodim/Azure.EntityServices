using Azure.Data.Tables;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace Azure.EntityServices.Tables.Extensions
{
    public static class EntityTableServicesCollectionExtensions
    {
        public static IServiceCollection AddEntityTableClient<T>(this IServiceCollection services,
            EntityTableClientOptions options,
            Action<EntityTableClientConfig<T>> configurator)
            where T : class, new()
        {
            services.AddTableClientService<T>(options.ConnectionString);

            var config = new EntityTableClientConfig<T>();
            configurator.Invoke(config);

            services.AddTransient(sp =>
            {
                var factory = sp.GetRequiredService<IAzureClientFactory<TableServiceClient>>();
                var client = factory.CreateClient(typeof(T).Name);
                return EntityTableClient.Create<T>(
                    factory
                    .CreateClient(typeof(T).Name))
                    .Configure(options, config);
            });

            return services;
        }

        private static IServiceCollection AddTableClientService<T>(this IServiceCollection services, string connectionString)
        {
            services.AddAzureClients(clientBuilder =>
            {
                clientBuilder
                .AddTableServiceClient(connectionString)
                .ConfigureOptions(options =>
                {
                    options.Diagnostics.IsLoggingContentEnabled = false;
                    options.Diagnostics.IsDistributedTracingEnabled = false;
                    options.Diagnostics.IsLoggingEnabled = false;
                    options.Diagnostics.IsLoggingContentEnabled = false;
                    options.Diagnostics.IsTelemetryEnabled = false;
                    
                })
                .WithName(typeof(T).Name);
            });
            return services;
        }
    }
}