using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public class ServiceBag<K, T> : IServiceBag<K, T>, IServiceBagBuilder<K, T>
    {
        private readonly IServiceProvider _provider; 

        protected  Func<K, T> KeyedFactory { get;  set; }

        public ServiceBag(IServiceProvider provider)
        {
            _provider = provider;
        }

        public T Get(K key) => KeyedFactory.Invoke(key);

        public void SetupFactory(Func<K, IServiceProvider, T> factory)
        {
            KeyedFactory = (k) => factory.Invoke(k, _provider);
        }
    }
}