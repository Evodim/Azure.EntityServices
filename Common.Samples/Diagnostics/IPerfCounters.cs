namespace Common.Samples.Diagnostics
{
    using System.Collections.Concurrent;

    public interface IPerfCounters
    {
        ConcurrentDictionary<string, IPerfCounter> Get();

        long In(string name);

        PerfMesure Mesure(string name);

        long Out(string name);

        void Clear();
    }
}