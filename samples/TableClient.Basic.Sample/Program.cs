using System.Threading.Tasks;

namespace TableClient.BasicSample
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