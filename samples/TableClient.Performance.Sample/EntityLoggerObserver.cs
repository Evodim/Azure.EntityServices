using Azure.EntityServices.Tables;
using Common.Samples.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace TableClient.PerformanceSample
{
    public class EntityLoggerObserver<T> : IEntityObserver<T>
        where T : class
    {
        private long _added = 0;
        private long _upserted = 0;
        private long _deleted = 0;
        private static readonly object ConsoleWriterLock = new object();
        private static bool started = false;

        public EntityLoggerObserver()
        {
            if (!started)
            {
                Console.WriteLine("|----------------------------------|");
                Console.WriteLine();
                Console.WriteLine("|----------------------------------|");
            }
            started = true;
        }

        public ConcurrentDictionary<string, PersonEntity> Persons = new ConcurrentDictionary<string, PersonEntity>();

        public string Name { get; set; }

        protected virtual void LogToConsole()
        {
            lock (ConsoleWriterLock)
            {
                var current = Console.GetCursorPosition();
                var trace = $" Add: {_added:0000} Upd: {_upserted:0000} Del: {_deleted:0000} ";
                Console.CursorTop = 1;
                Console.CursorLeft = 1;
                Console.Write(trace);
                Console.SetCursorPosition(current.Left, current.Top);
            }
        }

        public Task OnCompletedAsync()
        {
            LogToConsole();

            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception ex)
        {
            Console.WriteLine(ex.Message);
            throw ex;
        }

        public Task OnNextAsync(IEnumerable<EntityOperationContext<T>> contextBatch)
        {
            foreach (var context in contextBatch)
            {
                //ignore indexed tags changes
                if (context.RowKey.StartsWith("~") ||
                    context.PartitionKey.StartsWith("~"))
                {
                    continue;
                }

                switch (context.EntityOperation)

                {
                    case EntityOperationType.Delete:
                        Interlocked.Increment(ref _deleted);
                        break;

                    case EntityOperationType.Add:
                        Interlocked.Increment(ref _added);
                        break;

                    case EntityOperationType.AddOrMerge:
                    case EntityOperationType.AddOrReplace:
                    case EntityOperationType.Merge:
                    case EntityOperationType.Replace:
                        Interlocked.Increment(ref _upserted);
                        break;

                    default: break;
                }
            }
            if (_added % 1000 == 0 ||
                _deleted % 1000 == 0 ||
                _upserted % 1000 == 0)
            {
                LogToConsole();
            }
            return Task.CompletedTask;
        }
    }
}