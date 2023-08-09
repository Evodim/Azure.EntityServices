using Azure.EntityServices.Tables;
using Common.Samples.Models;
using Microsoft.Extensions.Azure;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TableClient.DependencyInjection.AdvancedSample
{/// <summary>
 /// Example of IEntityObserver implementation with DI
 /// </summary>
 /// <remarks>This is not a real worl example to create and maintain a projection updated from source table entity.
 /// You should apply asynchronous and decloupled side effects according to CQRS patterns
 /// it's common to use messaging to process commands and publish update events
 /// </remarks>
    public class SampleProjectionObserver : IEntityObserver<PersonEntity>
    {
        private readonly ConcurrentQueue<PersonEntity> _addOperations = new ConcurrentQueue<PersonEntity>();
        private readonly ConcurrentQueue<PersonEntity> _updateOperations = new ConcurrentQueue<PersonEntity>();
        private readonly ConcurrentQueue<PersonEntity> _deleteOperations = new ConcurrentQueue<PersonEntity>();
        private readonly IEntityTableClient<PersonEntity> _entityClient;

        private long added = 0;
        private long updated = 0;
        private long deleted = 0;

        public string Name { get; set; }

        public SampleProjectionObserver()
        {
        }

        public SampleProjectionObserver(IAzureClientFactory<IEntityTableClient<PersonEntity>> factory)
        { 
            _entityClient = factory.CreateClient("ProjectionClient");
        }

        public async Task OnCompletedAsync()
        {
            Interlocked.Exchange(ref added, _addOperations.Count + added);
            Interlocked.Exchange(ref updated, _updateOperations.Count + updated);
            Interlocked.Exchange(ref deleted, _deleteOperations.Count + deleted);
            Console.Write($"{nameof(SampleProjectionObserver)} ToAdd:    {added}     ToUpdate:    {updated}    ToDelete:  {deleted}        ");

            if (_addOperations.Count > 0)
            {
                await _entityClient.AddOrReplaceManyAsync(_addOperations.ToList());
                Console.WriteLine($"{_addOperations.Count} added");
                _addOperations.Clear();
            }
            if (_updateOperations.Count > 0)
            {
                await _entityClient.AddOrReplaceManyAsync(_updateOperations.ToList());
                Console.WriteLine($"{_updateOperations.Count} updated");
                _updateOperations.Clear();
            }

            if (_deleteOperations.Count > 0)
            {
                await _entityClient.DeleteManyAsync(_deleteOperations.ToList());
                Console.WriteLine($"{_deleteOperations.Count} deleted");
                _deleteOperations.Clear();
            }
            Console.SetCursorPosition(5, 5);
        }

        public Task OnErrorAsync(Exception ex)
        {
            Console.WriteLine(ex.Message);
            return Task.CompletedTask;
        }

        public Task OnNextAsync(IEnumerable<IEntityContext<PersonEntity>> contextBatch)
        {
            foreach (var context in contextBatch)
            {
                var entity = context.EntityDataReader.Read();

                switch (context.EntityOperation)
                {
                    case EntityOperation.Add:
                        _addOperations.Enqueue(entity);
                        break;

                    case EntityOperation.AddOrReplace:
                    case EntityOperation.AddOrMerge:
                    case EntityOperation.Merge:
                    case EntityOperation.Replace:
                        _updateOperations.Enqueue(entity);
                        break;

                    case EntityOperation.Delete:
                        _deleteOperations.Enqueue(entity);
                        break;
                }
            }
            return Task.CompletedTask;
        }
    }
}