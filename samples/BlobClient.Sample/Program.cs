using Azure.Storage.Blobs;
using System.Threading.Tasks;

namespace TableClient.Performance.Sample
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