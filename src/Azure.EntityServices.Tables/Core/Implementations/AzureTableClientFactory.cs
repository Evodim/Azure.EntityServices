using Azure.Data.Tables;
using Azure.EntityServices.Tables.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Core.Implementations
{
    public class AzureTableClientFactory : ITableClientFactory
      
    {
        private readonly TableServiceClient _tableServiceClient;

        public AzureTableClientFactory(TableServiceClient tableServiceClient)
        {
            _tableServiceClient = tableServiceClient;
        }

        public virtual ITableClientFacade<T> Create<T>(
            EntityTableClientConfig<T> config,
            EntityTableClientOptions options,
            Func<EntityTransactionGroup,Task<EntityTransactionGroup>> preProcessor,
            IEntityAdapter<T> entityAdapter,
            Func<IEnumerable<EntityOperation>, Task> onTransactionSubmittedHandler = null)
              where T : class, new()
        {
            
            return new TableClientFacade<T>(
               new AzureNativeTableClient<T>(options, entityAdapter, _tableServiceClient),       
               
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