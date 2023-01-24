using System.Threading.Tasks;

namespace TableClient.Basic.Sample
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