using Azure.Data.Tables;
using Azure.EntityServices.Tables.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Core.Implementations
{
    public class AzureTableBatchClientFactory<T> : ITableBatchClientFactory<T>
        where T : class, new()
    {
        private readonly TableServiceClient _tableServiceClient;

        public AzureTableBatchClientFactory(TableServiceClient tableServiceClient)
        {
            _tableServiceClient = tableServiceClient;
        }

        public ITableBatchClient<T> Create(
            EntityTableClientOptions options,
            Func<EntityTransactionGroup,
            Task<EntityTransactionGroup>> preProcessor,
            IEntityAdapter<T> entityAdapter,
            Func<IEnumerable<EntityOperation>, Task> onTransactionSubmittedHandler = null)
        {
            return new AzureTableBatchClient<T>(
               _tableServiceClient,
               new TableBatchClientOptions()
               {
                   TableName = options.TableName,
                   MaxItemInBatch = options.MaxItemToGroup,
                   MaxItemInTransaction = options.MaxOperationPerTransaction,
                   MaxParallelTasks = options.MaxParallelTransactions == -1 ? Environment.ProcessorCount : options.MaxParallelTransactions,
                   CreateTableIfNotExists = options.CreateTableIfNotExists
               },
                  preProcessor,
                  entityAdapter,
                  onTransactionSubmittedHandler

               )
            { };
        }
    }
}