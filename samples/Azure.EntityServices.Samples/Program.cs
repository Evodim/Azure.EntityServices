using System.Threading.Tasks;

namespace Azure.EntityServices.Samples
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