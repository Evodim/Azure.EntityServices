using System;

namespace Azure.EntityServices.Samples.Diagnostics
{
    public interface IPerfCounter
    {
        string Context { get; }
        string Name { get; }
        long InCount { get; }
        long OutCount { get; }
        TimeSpan TotalDuration { get; }
        TimeSpan AverageDuration { get; }
        TimeSpan MinDuration { get; }
        TimeSpan MaxDuration { get; }

        long In();

        long Out();
    }
}