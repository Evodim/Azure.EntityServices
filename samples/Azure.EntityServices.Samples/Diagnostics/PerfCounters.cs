using Azure.EntityServices.SamplesDiagnostics;
using System.Collections.Concurrent;

namespace Azure.EntityServices.Samples.Diagnostics
{
    public class PerfCounters : IPerfCounters
    {
        private readonly string _context;
        private readonly ConcurrentDictionary<string, IPerfCounter> _blockCounters = new();

        public PerfCounters(string context)
        {
            _context = context;
        }

        public long In(string name)
        {
            if (!_blockCounters.ContainsKey(name)) _blockCounters.TryAdd(name, new PerfCounter(_context, name));
            return _blockCounters[name].In();
        }

        public long Out(string name)
        {
            if (!_blockCounters.ContainsKey(name)) _blockCounters.TryAdd(name, new PerfCounter(_context, name));
            return _blockCounters[name].Out();
        }

        public PerfMesure Mesure(string name)
        {
            return new PerfMesure(this, name);
        }

        public ConcurrentDictionary<string, IPerfCounter> Get()
        {
            return _blockCounters;
        }

        public void Clear()
        {
            _blockCounters.Clear();
        }
    }
}