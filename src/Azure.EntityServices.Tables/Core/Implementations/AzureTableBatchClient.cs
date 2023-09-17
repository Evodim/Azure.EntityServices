using Azure.Data.Tables;
using Azure.EntityServices.Tables.Core.Abstractions;
using Azure.EntityServices.Tables.Extensions;
using Polly;
using Polly.Retry;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Core.Implementations
{
    public class AzureTableBatchClient<T> : BaseTableBatchClient<T> where T : class, new()
    {
        private readonly AsyncRetryPolicy _retryPolicy;
        private readonly TableClient _client;
        private readonly TableServiceClient _tableClientService;

        public AzureTableBatchClient(
            TableServiceClient tableServiceClient,
            TableBatchClientOptions options,
            Func<EntityTransactionGroup, Task<EntityTransactionGroup>> preProcessor,
            IEntityAdapter<T> entityAdapter,
            Func<IEnumerable<EntityOperation>, Task> onTransactionSubmittedHandler = null) : base(
                options,
                preProcessor,
                entityAdapter,
                onTransactionSubmittedHandler)
        {
            _tableClientService = tableServiceClient;

            _retryPolicy = Policy.Handle<RequestFailedException>(ex => ex.HandleAzureStorageException(options.TableName, tableServiceClient, options.CreateTableIfNotExists))
              .WaitAndRetryAsync(5, i => TimeSpan.FromSeconds(2 * i));

            _client = _tableClientService.GetTableClient(options.TableName);
        }

        protected override Task SendBulkOperations(IEnumerable<EntityOperation> entityOperations)
        {
            return _retryPolicy.ExecuteAsync(() => _client.SubmitTransactionAsync(entityOperations.Select(op =>
            {
                return new TableTransactionAction(op.EntityOperationType.MapToTableTransactionActionType(),
                   op.ToTableEntityModel<T>()
                    );
            })));
        }
    }
}