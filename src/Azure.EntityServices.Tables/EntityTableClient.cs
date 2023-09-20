using Azure.Data.Tables;
using Azure.EntityServices.Core.Abstractions;
using Azure.EntityServices.Tables.Core.Abstractions;
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
        private readonly TableServiceClient _tableServiceClient;

        internal EntityTableClient(TableServiceClient tableService)
        {
            _tableServiceClient = tableService;
        }

        public override void ConfigureServices( 
            IEntityAdapter<T> entityAdapter,
            ITableClientFactory<T> tableClientFactory)
        {
           
            base.ConfigureServices( 
                new AzureTableEntityAdapter<T>(EntityKeyBuilder, Config, Options), 
                new AzureTableClientFactory<T>(_tableServiceClient));
        }
    }
}