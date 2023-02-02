using System.Threading.Tasks;

namespace TableClient.Basic.Sample
{
    public static class Program
    {
        private static async Task Main()
        {
            await BasicSample.Run();
            System.Console.ReadLine();
        }
    }
}