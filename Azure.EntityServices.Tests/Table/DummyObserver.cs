using Azure.EntityServices.Table;
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
            if (operation.TableOperation == EntityOperation.Replace)
            {
                Persons.TryAdd(operation.Partition + operation.Entity.PersonId, operation.Entity);
                Interlocked.Increment(ref _created);
            }
            if (operation.TableOperation == EntityOperation.Delete)
            {
                Persons.Remove(operation.Partition + operation.Entity.PersonId, out var _);
                Interlocked.Increment(ref _deleted);
            }
        }
    }
}