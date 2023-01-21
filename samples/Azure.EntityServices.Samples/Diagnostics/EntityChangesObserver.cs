using Azure.EntityServices.Table.Common.Models;
using Azure.EntityServices.Tables;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Samples.Diagnostics
{
    public class EntityLoggerObserver<T> : IEntityObserver<T>
        where T : class
    {
        private long _added = 0;
        private long _upserted = 0;
        private long _deleted = 0;
        private static readonly object ConsoleWriterLock = new object();
        public EntityLoggerObserver()
        {

        }
        public ConcurrentDictionary<string, PersonEntity> Persons = new();

        protected virtual void LogToConsole()
        {
            lock (ConsoleWriterLock)
            {
                var current = Console.GetCursorPosition();
                Console.CursorLeft += Console.WindowWidth - 30;
                Console.WriteLine($"** EntityLoggerObserver **");
                Console.CursorLeft += Console.WindowWidth - 30;
                Console.WriteLine($"Add: {_added} Upsrt: {_upserted} Del: {_deleted}");
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

        public Task OnNextAsync(IEnumerable<IEntityBinderContext<T>> contextBatch)
        {
            foreach (var context in contextBatch)
            {
                //ignore indexed tags changes
                if (context.EntityBinder.RowKey.StartsWith("~") ||
                    context.EntityBinder.PartitionKey.StartsWith("~"))
                {
                    continue;
                }

                switch (context.EntityOperation)

                {
                    case EntityOperation.Delete:
                        Interlocked.Increment(ref _deleted);
                        break;

                    case EntityOperation.Add:
                        Interlocked.Increment(ref _added);
                        break;
                    case EntityOperation.AddOrMerge:
                    case EntityOperation.AddOrReplace:
                    case EntityOperation.Merge:
                    case EntityOperation.Replace:
                        Interlocked.Increment(ref _upserted);
                        break;

                    default: break;
                }
            }
            if (_added % 1000 ==0 ||
                _deleted % 1000 == 0 ||
                _upserted % 1000 == 0)
            {
                LogToConsole();
            }
            return Task.CompletedTask;
        }
    }
}