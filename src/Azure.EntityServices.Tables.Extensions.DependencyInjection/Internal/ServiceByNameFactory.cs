using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public class ServiceByNameFactory<TService> : IServiceByNameFactory<TService>
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly IDictionary<string, Type> _registrations;

        public ServiceByNameFactory(IServiceProvider serviceProvider, IDictionary<string, Type> registrations)
        {
            _serviceProvider = serviceProvider;
            _registrations = registrations;
        }

        public TService GetByName(string name)
        {
            if (!_registrations.TryGetValue(name, out var implementationType))
                throw new ArgumentException($"Service name '{name}' is not registered");
            return (TService)_serviceProvider.GetService(implementationType);
        }

        public TService GetRequiredByName(string name)
        {
            if (!_registrations.TryGetValue(name, out var implementationType))
                throw new ArgumentException($"Service name '{name}' is not registered");
            return (TService)_serviceProvider.GetRequiredService(implementationType);
        }

        public ICollection<string> GetNames()
        {
            return _registrations.Keys;
        }
    }
}