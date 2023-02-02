using Azure.EntityServices.Tables;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace TableClient.DependencyInjection.Sample
{
    public class GenericEntityObserver<T> : IEntityObserver<T>
        where T : class, new()
    {
        private ConcurrentQueue<T> _addOperations = new ConcurrentQueue<T>();
        private ConcurrentQueue<T> _updateOperations = new ConcurrentQueue<T>();
        private ConcurrentQueue<T> _deleteOperations = new ConcurrentQueue<T>();

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
            var position = Console.GetCursorPosition();

            Console.SetCursorPosition(0, 2);
            Console.WriteLine($"GenericEntityObserver Add:{added:G6} Upt: {updated:G6} Del: {deleted:G6}");
            Console.SetCursorPosition(position.Left, position.Top);

            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception error)
        {
            return Task.CompletedTask;
        }

        public Task OnNextAsync(IEnumerable<IEntityBinderContext<T>> contextBatch)
        {
            foreach (var context in contextBatch)
            { 
                if (!context.EntityBinder.RowKey.StartsWith("~"))
                  {
                    continue;
                   }
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
            if (_addOperations.Count > 1000 || _updateOperations.Count > 1000)
            {
                return OnCompletedAsync();
            }
            return Task.CompletedTask;
        }
    }
}