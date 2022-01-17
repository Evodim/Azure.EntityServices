using System;
using System.Threading.Tasks;
namespace Azure.EntityServices.Tests.Table
{
    internal class DisposableContext<T> : IDisposable
    {
        private bool _disposed = false;

        private readonly Func<T, Task> _disposer;
        private readonly T _value;

        public T Value => _value;

        public DisposableContext(T value, Func<T, Task> disposer)
        {
            _disposer = disposer;
            _value = value;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                // invoke disposer
                _disposer.Invoke(_value).GetAwaiter().GetResult();
            }
            _disposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}