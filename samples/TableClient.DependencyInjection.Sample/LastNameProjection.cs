﻿using Azure.EntityServices.Tables;
using Common.Samples.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TableClient.DependencyInjection.Sample
{
    public class SampleProjectionObserver : IEntityObserver<PersonEntity>
    {
        private ConcurrentQueue<PersonEntity> _addOperations = new ConcurrentQueue<PersonEntity>();
        private ConcurrentQueue<PersonEntity> _updateOperations = new ConcurrentQueue<PersonEntity>();
        private ConcurrentQueue<PersonEntity> _deleteOperations = new ConcurrentQueue<PersonEntity>();
        private readonly IEntityTableClient<PersonEntity> _client;
        private readonly Func<IEntityTableClient<PersonEntity>> _clientFactory;

        private long added = 0;
        private long updated = 0;
        private long deleted = 0;

        public SampleProjectionObserver()
        {
        }

        public SampleProjectionObserver(EntityTableClientOptions options)
        {
            _client = EntityTableClient.Create<PersonEntity>(
               options,
                config =>
                {
                    config
                        .AddComputedProp("PersonId", p => $"{p.LastName}~{p.PersonId}")
                        .SetPartitionKey(e => $"~LastName-{e.LastName[..2]}")
                        .SetPrimaryKey(p => $"{p.LastName}~{p.PersonId}");
                });

            _clientFactory = () =>
              EntityTableClient.Create<PersonEntity>(
               options,
                config =>
                {
                    config
                     .AddComputedProp("PersonId", p => $"{p.LastName}~{p.PersonId}")
                     .SetPartitionKey(e => $"~LastName-{e.LastName[..2]}")
                     .SetPrimaryKey(p => $"{p.LastName}~{p.PersonId}");
                });
        }

        public SampleProjectionObserver(IEntityTableClient<PersonEntity> entityTableClient)
        {
            _clientFactory = () => entityTableClient;
        }

        public async Task OnCompletedAsync()
        {
            Console.SetCursorPosition(5, 5);
            Interlocked.Exchange(ref added, _addOperations.Count + added);
            _addOperations.Clear();
            Interlocked.Exchange(ref updated, _updateOperations.Count + updated);
            _updateOperations.Clear();
            Interlocked.Exchange(ref deleted, _deleteOperations.Count + deleted);
            _deleteOperations.Clear();
            Console.Write($"Added:    {added}     Updated:    {updated}    Deleted:  {deleted}        ");

            return;

            var client = _clientFactory();

            if (_addOperations.Count > 0)
            {
                await client.AddOrReplaceManyAsync(_addOperations.ToList());
                Console.WriteLine($"{_addOperations.Count} added");
                _addOperations.Clear();
            }
            if (_updateOperations.Count > 0)
            {
                await client.AddOrReplaceManyAsync(_updateOperations.ToList());
                Console.WriteLine($"{_updateOperations.Count} updated");
                _updateOperations.Clear();
            }

            if (_deleteOperations.Count > 0)
            {
                await client.DeleteManyAsync(_deleteOperations.ToList());
                Console.WriteLine($"{_deleteOperations.Count} deleted");
                _deleteOperations.Clear();
            }
        }

        public Task OnErrorAsync(Exception error)
        {
            return Task.CompletedTask;
        }

        public Task OnNextAsync(IEnumerable<IEntityBinderContext<PersonEntity>> contextBatch)
        {
            foreach (var context in contextBatch)
            {
                var entity = context.EntityBinder.UnBind();

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