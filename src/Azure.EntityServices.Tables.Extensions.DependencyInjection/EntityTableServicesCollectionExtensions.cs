using Azure.Data.Tables;
using Azure.EntityServices.Tables.Extensions.DependencyInjection;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using static System.Net.Mime.MediaTypeNames;

namespace Azure.EntityServices.Tables.Extensions
{
    public class ServicebagContainer<T>
    {
        public Dictionary<string, Type> Bags { get; }
        public ServicebagContainer()
        {
            Bags= new Dictionary<string, Type>(); 
        }
        public void AddServiceImplementation<TImplementation>(string name)
            where TImplementation: T
        {
            Bags.Add(name, typeof(TImplementation));
        }
    }

    public static class EntityTableServicesCollectionExtensions
    { 
        public static IServiceCollection AddEntityTableClient<T>(this IServiceCollection services,
        EntityTableClientOptions options,
        Action<EntityTableClientConfigBuilder<T>> configBuilder,
        Action<TableClientOptions> tableClientOptionsAction = null
        )
        where T : class, new()
        {
            services.AddTableClientService<T>(options.ConnectionString, tableClientOptionsAction);
            var entityObserverBuilder = services.AddByName<IEntityObserver<T>>();
      
            services.AddTransient<IEntityTableClient<T>>(sp =>
            {
                var config = new EntityTableClientConfigBuilder<T>(sp, entityObserverBuilder); 
              
                configBuilder.Invoke(config);
               
                var tableServiceFactory = sp.GetRequiredService<IAzureClientFactory<TableServiceClient>>(); 
                var client = tableServiceFactory.CreateClient(typeof(T).Name); 
               
                return EntityTableClient
                .Create<T>(client)
                .Configure(options, config);
            });

            entityObserverBuilder.Build();
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