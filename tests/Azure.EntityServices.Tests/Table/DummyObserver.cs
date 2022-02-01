using Azure.EntityServices.Tables;
using Azure.EntityServices.Tests.Common.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Azure.EntityServices.Tests.Table
{
    public class DummyObserver : IEntityObserver<PersonEntity>
    {
        private long _created = 0;
        private long _deleted = 0;
        public long CreatedCount => _created;
        public long DeletedCount => _deleted;

        public ConcurrentDictionary<string, PersonEntity> Persons = new();

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotSupportedException();
        }

        public void OnNext(IEntityOperationContext<PersonEntity> operation)
        {
            switch(operation.TableOperation)

            {
                case EntityOperation.Delete:
                    Persons.Remove(operation.Partition + operation.Entity.PersonId, out var _);
                    Interlocked.Increment(ref _deleted);
                    break;
                case EntityOperation.Add:
                case EntityOperation.AddOrMerge:
                case EntityOperation.AddOrReplace:
                    Persons.TryAdd(operation.Partition + operation.Entity.PersonId, operation.Entity);
                    Interlocked.Increment(ref _created);
                    break;
                case EntityOperation.Merge:
                case EntityOperation.Replace:
                    
                default:break;

            }
       
        }
    }
}