using System.Threading.Tasks;

namespace TableClient.Performance.Sample
{
    public static class Program
    {
        private static async Task Main()
        {
            await TableSample.Run();
            System.Console.ReadLine();
        }
    }
}