using Azure.EntityServices.Tables.Core.Abstractions;

namespace Azure.EntityServices.Tables.Core.Implementations
{
    public class AzureTableEntityAdapterFactory : IEntityAdapterFactory
    {
        public virtual IEntityAdapter<T> Create<T>(
            EntityKeyBuilder<T> entityKeyBuilder,
            EntityTableClientConfig<T> entityTableClientConfig,
            EntityTableClientOptions options) where T : class, new()
        {
           return new AzureTableEntityAdapter<T>(entityKeyBuilder, entityTableClientConfig, options);
        }
    }
}