using Azure.EntityServices.Tables.Core;

namespace Azure.EntityServices.Tables.Core.Abstractions
{
    public interface IEntityAdapterFactory
    {
        IEntityAdapter<T> Create<T>(
            EntityKeyBuilder<T> entityKeyBuilder,
            EntityTableClientConfig<T> entityTableClientConfig,
            EntityTableClientOptions options) where T : class, new();
    }
}