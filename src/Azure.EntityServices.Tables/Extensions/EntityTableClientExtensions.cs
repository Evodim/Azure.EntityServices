using System;

namespace Azure.EntityServices.Tables
{
    public static class EntityTableClientExtensions
    {
        public static EntityTableClient<T> Configure<T>(this EntityTableClient<T> entityClient,Action<EntityTableClientOptions> optionsDelegate, Action<EntityTableClientConfig<T>> configurator)
         where T : class, new()
        {
            _ = optionsDelegate ?? throw new ArgumentNullException(nameof(optionsDelegate));
            _ = configurator ?? throw new ArgumentNullException(nameof(configurator));

            var options = new EntityTableClientOptions();
            var configuration = new EntityTableClientConfig<T>();

            optionsDelegate.Invoke(options);
            configurator.Invoke(configuration);

            return entityClient.Configure(options, configuration);
        }

        public static EntityTableClient<T> Configure<T>(this EntityTableClient<T> entityClient, EntityTableClientOptions options, Action<EntityTableClientConfig<T>> configurator)
        where T : class, new()
        {
            _ = options ?? throw new ArgumentNullException(nameof(options));
            _ = configurator ?? throw new ArgumentNullException(nameof(configurator));

            var configuration = new EntityTableClientConfig<T>();

            configurator.Invoke(configuration);

            return entityClient.Configure(options, configuration);
        }

        public static EntityTableClient<T> Configure<T>(this EntityTableClient<T> entityClient, EntityTableClientOptions options, EntityTableClientConfig<T> configuration)
        where T : class, new()
        {
            _ = options ?? throw new ArgumentNullException(nameof(options));
            _ = configuration ?? throw new ArgumentNullException(nameof(configuration));

            return entityClient.Configure(options, configuration);
        }
    }
}