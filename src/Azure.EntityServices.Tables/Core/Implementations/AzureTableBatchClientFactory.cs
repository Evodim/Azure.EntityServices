using Azure.Data.Tables;
using Azure.EntityServices.Tables.Core;
using Azure.EntityServices.Tables.Core.Abstractions;
using System;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Core.Implementations
{
    public class AzureTableBatchClientFactory<T> : INativeTableBatchClientFactory<T>
        where T : class, new()
    {
        private readonly TableServiceClient _tableServiceClient;

        public AzureTableBatchClientFactory(TableServiceClient tableServiceClient)
        {
            _tableServiceClient = tableServiceClient;
        }

        public INativeTableBatchClient<T> Create(
            EntityTableClientOptions options,
            Func<EntityTransactionGroup,
            Task<EntityTransactionGroup>> preProcessor,
            IEntityAdapter<T> entityAdapter,
            OnTransactionSubmitted onTransactionSubmittedHandler = null)
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