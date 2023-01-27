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
            Action<EntityTableClientConfig<T>> configurator,
            Action<TableClientOptions> tableClientOptionsAction = null
            )
            where T : class, new()
        {
            services.AddTableClientService<T>(options.ConnectionString, tableClientOptionsAction);

            var config = new EntityTableClientConfig<T>();
            configurator.Invoke(config);

            services.AddTransient(sp =>
            {
                var factory = sp.GetRequiredService<IAzureClientFactory<TableServiceClient>>();
                var client = factory.CreateClient(typeof(T).Name);
                return EntityTableClient.Create<T>(client)
                .Configure(options, config);
                 
            });

            return services;
        }
        private static IServiceCollection AddTableClientService<T>(this IServiceCollection services,
            string connectionString,
            Action<TableClientOptions> optionsAction = null)
        {
            services.AddAzureClients(clientBuilder =>
            {
                clientBuilder
                 .AddTableServiceClient(connectionString)
                 .ConfigureOptions(options=> optionsAction?.Invoke(options))
                 .WithName(typeof(T).Name);
            });
            return services;
        }
      }
}