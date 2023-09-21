using Azure.EntityServices.Tables;
using Common.Samples.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Table.Tests
{
    public class DummyObserver : IEntityObserver<PersonEntity>
    {
        private long _upserted = 0;
        private long _deleted = 0;
        public long CreatedCount => _upserted;
        public long DeletedCount => _deleted;

        public string Name { get; set; }

        public ConcurrentDictionary<string, PersonEntity> Persons = new();

        public Task OnCompletedAsync()
        {
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception ex)
        {
            Console.WriteLine(ex.Message);
            throw ex;
        }

        public Task OnNextAsync(IEnumerable<EntityOperationContext<PersonEntity>> contextBatch)
        {
            foreach (var context in contextBatch)
            {
                //ignore indexed tags changes
                if (context.RowKey.StartsWith("~") ||
                    context.PartitionKey.StartsWith("~"))
                {
                    continue;
                }
                var entity = context.EntityDataReader.Read();

                switch (context.EntityOperation)

                {
                    case EntityOperationType.Delete:
                        Persons.Remove(context.PartitionKey + entity.PersonId, out var _);
                        Interlocked.Increment(ref _deleted);
                        break;

                    case EntityOperationType.Add:
                    case EntityOperationType.AddOrMerge:
                    case EntityOperationType.AddOrReplace:
                        Persons.TryAdd(context.PartitionKey + entity.PersonId, entity);
                        Interlocked.Increment(ref _upserted);
                        break;

                    case EntityOperationType.Merge:
                    case EntityOperationType.Replace:

                    default: break;
                }
            }

            return Task.CompletedTask;
        }
    }
}