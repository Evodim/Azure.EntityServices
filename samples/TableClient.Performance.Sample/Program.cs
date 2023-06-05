using System.Threading.Tasks;

namespace TableClient.PerformanceSample
{
    public static class Program
    {
        private static async Task Main()
        {
            await SampleConsole.Run();
            System.Console.ReadLine();
        }
    }
}