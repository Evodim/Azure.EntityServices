using Azure.Data.Tables;
using Azure.EntityServices.Tables.Extensions.DependencyInjection;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace Azure.EntityServices.Tables.Extensions
{
    public static class EntityTableServicesCollectionExtensions
    {
        public static IServiceCollection AddEntityTableClient<T>(this IServiceCollection services, 
        Action<IEntityTableClientBuilder<T>> configBuilder,
        Action<TableClientOptions> tableClientOptionsAction = null
        )
        where T : class, new()
        {
            var buidler = new EntityTableClientBuilder<T>(services);

            configBuilder.Invoke(buidler);

            var options = buidler.BindOptions();

            services.AddTableClientService<T>(options.ConnectionString);

            services.AddTransient<IEntityTableClient<T>>(sp =>
            {
                var config = buidler.BindConfig(sp);
                var tableServiceFactory = sp.GetRequiredService<IAzureClientFactory<TableServiceClient>>();
                var tableService = tableServiceFactory.CreateClient(typeof(T).Name);
               
                return EntityTableClient
                .Create<T>(tableService)
                .Configure(options, config);
            });

            buidler.Build();
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
                 .ConfigureOptions(options => optionsAction?.Invoke(options))
                 .WithName(typeof(T).Name);
            });
            return services;
           
        }
    }
}