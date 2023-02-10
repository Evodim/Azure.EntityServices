using Common.Samples.Diagnostics;
using System;

namespace Common.Samples.Diagnostics
{
    public class PerfMesure : IDisposable
    {
        private readonly PerfCounters _counters;
        private readonly string _blockname;
        private bool _disposed = false;
        public PerfMesure(PerfCounters counters, string blockname)
        {
            _counters = counters;
            _blockname = blockname;
            _counters.In(_blockname);
        }

        public string Name => _blockname;

        protected virtual void Dispose(bool disposing)
        {

            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                // invoke disposer
                _counters.Out(_blockname);
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