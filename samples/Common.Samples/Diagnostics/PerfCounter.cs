using Common.Samples.Diagnostics;
using System;
using System.Threading;

namespace TableClient.Performance.SampleDiagnostics
{
 
    
    
    /// <example>
    /// //Basic example
    /// var counters = new MetricCounters("TestLive");
    /// using(var mesure = counters.Mesure("readByPartyId_Keyed"))
    ///	{
    ///	}
    /// foreach(var counter in counters.Get())
    /// {
    ///	Console.WriteLine($"{counter.Key} {counter.Value.Duration().TotalSeconds} seconds");
    /// }
    /// </example>    
    public class PerfCounter : IPerfCounter
    {
        private long _inCount = 0;
        private long _outCount = 0;
        private long _inTicks = 0;
        private long _outTicks = 0;

        private long _maxDuration = 0;
        private long _minDuration = 0;
        private long _averageDuration = 0;
        private readonly string _topic;
        private readonly string _name;

        string IPerfCounter.Name => _name;

        public long InCount => _inCount;
        public long OutCount => _outCount;

        public PerfCounter(string topic, string name)
        {
            _topic = topic;
            _name = name;
        }

        public long In()
        {
            Interlocked.Exchange(ref _inTicks, DateTimeOffset.UtcNow.Ticks);
            Interlocked.Exchange(ref _outTicks, DateTimeOffset.UtcNow.Ticks);
            return Interlocked.Increment(ref _inCount);
        }

        public long Out()
        {
            Interlocked.Exchange(ref _outTicks, DateTimeOffset.UtcNow.Ticks);
            UpdateState();
            return Interlocked.Increment(ref _outCount);
        }

        public TimeSpan TotalDuration => new TimeSpan(_outTicks - _inTicks);

        public TimeSpan AverageDuration => new TimeSpan(_averageDuration);

        public TimeSpan MinDuration => new TimeSpan(_minDuration);

        public TimeSpan MaxDuration => new TimeSpan(_maxDuration);

        public string Context => _topic;

        protected void UpdateState()
        {
            var duration = _outTicks - _inTicks;

            if (_maxDuration < duration) Interlocked.Exchange(ref _maxDuration, duration);
            if (_minDuration > duration) Interlocked.Exchange(ref _minDuration, duration);
            if (_averageDuration != 0)
                Interlocked.Exchange(ref _averageDuration, (_averageDuration + duration) / 2);
            else
                Interlocked.Exchange(ref _averageDuration, duration);
        }
    }
}