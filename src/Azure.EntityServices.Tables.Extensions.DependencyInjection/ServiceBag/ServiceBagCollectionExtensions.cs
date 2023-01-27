using Microsoft.Extensions.DependencyInjection;
using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public static class ServiceBagCollectionExtensions
    { 
        public static IServiceCollection AddTransientServiceBag<K,T>(this IServiceCollection services, Action<IServiceBagBuilder<K,T>> creator)
        where T : class
        {
            return services.AddTransient<IServiceBag<K,T>>(sp =>
            {
                var bag = new ServiceBag<K,T>(sp);
                creator.Invoke(bag);
                return bag;
            });
        }
        public static IServiceCollection AddScopedServiceBag<K, T>(this IServiceCollection services, Action<IServiceBagBuilder<K, T>> creator)
       where T : class
        {
            return services.AddScoped<IServiceBag<K, T>>(sp =>
            {
                var bag = new ServiceBag<K, T>(sp);
                creator.Invoke(bag);
                return bag;
            });
        }
        public static IServiceCollection AddSingleTonServiceBag<K, T>(this IServiceCollection services, Action<IServiceBagBuilder<K, T>> creator)
     where T : class
        {
            return services.AddSingleton<IServiceBag<K, T>>(sp =>
            {
                var bag = new ServiceBag<K, T>(sp);
                creator.Invoke(bag);
                return bag;
            });
        }
       
    }
}