using Azure.EntityServices.Samples.Diagnostics;
using System;
using System.Linq;
 

namespace Azure.EntityServices.Samples
{
    public static class PerfCountersExtensions
    {
        public static void WriteToConsole(this IPerfCounters counters)
        {

            foreach (var counter in counters.Get().OrderBy(c => c.Key))
            {
                WriteLineDuration($"{counter.Key} ", counter.Value);
            }
            Console.WriteLine("Finished");
            Console.ReadLine();
        }
        private static void WriteLineDuration(string text, IPerfCounter counter)
        {
            Console.Write(text);

            var prevColor = Console.ForegroundColor;
            Console.ForegroundColor = (counter.AverageDuration.TotalSeconds < 1) ? ConsoleColor.Green : ConsoleColor.Yellow;
            Console.WriteLine($"{Math.Round(counter.AverageDuration.TotalSeconds, 3)} seconds");

            Console.ForegroundColor = prevColor;
        }
    }
}
