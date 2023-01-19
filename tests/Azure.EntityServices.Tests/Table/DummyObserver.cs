using Azure.EntityServices.Tables;
using Azure.EntityServices.Table.Common.Models;
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

        public ConcurrentDictionary<string, PersonEntity> Persons = new();

        public Task OnCompletedAsync()
        {
            Console.WriteLine($"Upserted: {_upserted} Deleted: {_deleted}");
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception error)
        {
            throw error;
        }

        public Task OnNextAsync(IEnumerable<IEntityBinderContext<PersonEntity>> contextBatch)
        {
           
            foreach (var context in contextBatch)
            {
                //ignore indexed tags changes 
                if (context.EntityBinder.RowKey.StartsWith("~") ||
                    context.EntityBinder.PartitionKey.StartsWith("~"))
                {
                    continue;
                }    
                var entity = context.EntityBinder.UnBind();

                switch (context.EntityOperation)

                {
                    case EntityOperation.Delete:
                        Persons.Remove(context.EntityBinder.PartitionKey + entity.PersonId, out var _);
                        Interlocked.Increment(ref _deleted);
                        break;
                    case EntityOperation.Add:
                    case EntityOperation.AddOrMerge:
                    case EntityOperation.AddOrReplace:
                        Persons.TryAdd(context.EntityBinder.PartitionKey + entity.PersonId, entity);
                        Interlocked.Increment(ref _upserted);
                        break;
                    case EntityOperation.Merge:
                    case EntityOperation.Replace:

                    default: break;
                }
            }

            return Task.CompletedTask;
        }
    }
}