using System.Threading.Tasks;

namespace BlobClient.BasicSample
{
    public static class Program
    {
        private static async Task Main()
        {
            await BlobSample.Run();
            System.Console.ReadLine();
        }
    }
}