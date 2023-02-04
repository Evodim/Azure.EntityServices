using System;

namespace Azure.EntityServices.Blobs
{
    public static class EntityBlobClientExtensions
    {
        public static EntityBlobClient<T> Configure<T>(this EntityBlobClient<T> entityClient, Action<EntityBlobClientOptions> optionsDelegate, Action<EntityBlobClientConfig<T>> configurator)
         where T : class, new()
        {
            _ = optionsDelegate ?? throw new ArgumentNullException(nameof(optionsDelegate));
            _ = configurator ?? throw new ArgumentNullException(nameof(configurator));

            var options = new EntityBlobClientOptions();
            var configuration = new EntityBlobClientConfig<T>();

            optionsDelegate.Invoke(options);
            configurator.Invoke(configuration);

            return entityClient.Configure(options, configuration);
        }

        public static EntityBlobClient<T> Configure<T>(this EntityBlobClient<T> entityClient, EntityBlobClientOptions options, Action<EntityBlobClientConfig<T>> configurator)
        where T : class, new()
        {
            _ = options ?? throw new ArgumentNullException(nameof(options));
            _ = configurator ?? throw new ArgumentNullException(nameof(configurator));

            var configuration = new EntityBlobClientConfig<T>();

            configurator.Invoke(configuration);

            return entityClient.Configure(options, configuration);
        }

        public static EntityBlobClient<T> Configure<T>(this EntityBlobClient<T> entityClient, EntityBlobClientOptions options, EntityBlobClientConfig<T> configuration)
        where T : class, new()
        {
            _ = options ?? throw new ArgumentNullException(nameof(options));
            _ = configuration ?? throw new ArgumentNullException(nameof(configuration));

            return entityClient.Configure(options, configuration);
        }
    }
}
