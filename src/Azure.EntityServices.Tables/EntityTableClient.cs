using Azure.Data.Tables;
using Azure.EntityServices.Core.Abstractions;
using Azure.EntityServices.Tables.Core.Implementations;

namespace Azure.EntityServices.Tables
{
    /// <summary>
    /// Client to manage entities in azure Tables
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class EntityTableClient<T> : BaseEntityTableClient<T>
    where T : class, new()
    {
        public EntityTableClient(TableServiceClient tableService)
        : base(
                 new AzureTableEntityAdapterFactory(),
                 new AzureTableClientFactory(tableService)
                 )
        {
        }
    }
}