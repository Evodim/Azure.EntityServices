using Azure.EntityServices.Tables;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace TableClient.DependencyInjection.AdvancedSample
{
    public class GenericEntityObserver<T> : IEntityObserver<T>
        where T : class, new()
    {
        private readonly ConcurrentQueue<T> _addOperations = new();
        private readonly ConcurrentQueue<T> _updateOperations = new();
        private readonly ConcurrentQueue<T> _deleteOperations = new();

        private long added = 0;
        private long updated = 0;
        private long deleted = 0;

        public GenericEntityObserver()
        {
        }

        public string Name { get; set; }

        public Task OnCompletedAsync()
        {
            Interlocked.Exchange(ref added, _addOperations.Count + added);
            _addOperations.Clear();
            Interlocked.Exchange(ref updated, _updateOperations.Count + updated);
            _updateOperations.Clear();
            Interlocked.Exchange(ref deleted, _deleteOperations.Count + deleted);
            _deleteOperations.Clear();
            var (Left, Top) = Console.GetCursorPosition();

            Console.SetCursorPosition(0, 2);
            Console.WriteLine($"GenericEntityObserver Add:{added:G6} Upt: {updated:G6} Del: {deleted:G6}");
            Console.SetCursorPosition(Left, Top);

            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception error)
        {
            return Task.CompletedTask;
        }

        public Task OnNextAsync(IEnumerable<EntityOperationContext<T>> contextBatch)
        {
            foreach (var context in contextBatch)
            {
                if (!context.RowKey.StartsWith("~"))
                {
                    continue;
                }
                var entity = context.EntityDataReader.Read();

                switch (context.EntityOperation)
                {
                    case EntityOperationType.Add:
                        _addOperations.Enqueue(entity);
                        break;

                    case EntityOperationType.AddOrReplace:
                    case EntityOperationType.AddOrMerge:
                    case EntityOperationType.Merge:
                    case EntityOperationType.Replace:
                        _updateOperations.Enqueue(entity);
                        break;

                    case EntityOperationType.Delete:
                        _deleteOperations.Enqueue(entity);
                        break;
                }
            }
            if (_addOperations.Count > 1000 || _updateOperations.Count > 1000)
            {
                return OnCompletedAsync();
            }
            return Task.CompletedTask;
        }
    }
}