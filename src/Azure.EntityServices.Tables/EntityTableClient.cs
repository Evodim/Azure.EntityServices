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
            INativeTableClient<T> nativeTableClient,
            IEntityAdapter<T> entityAdapter,
            ITableBatchClientFactory<T> nativeTableBatchClientFactory)
        {
            nativeTableBatchClientFactory = new AzureTableBatchClientFactory<T>(_tableServiceClient);

            entityAdapter = new AzureTableEntityAdapter<T>(
             EntityKeyBuilder,
             Config,
             Options);

            nativeTableClient = new AzureTableClient<T>(Options, entityAdapter, _tableServiceClient);

            base.ConfigureServices(nativeTableClient, entityAdapter, nativeTableBatchClientFactory);
        }
    }
}