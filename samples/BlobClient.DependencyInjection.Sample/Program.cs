using Azure.EntityServices.Blobs; 
using Azure.EntityServices.Blobs.Extensions.DependencyInjection;
using Common.Samples;
using Common.Samples.Models;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace BlobClient.BasicSample
{
    public static class Program
    {
        public static void Main()
        {
            CreateHostBuilder(null).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
           .ConfigureHostConfiguration(configuration =>
           {
           })
           .ConfigureServices((hostContext, services) =>
           {
               services.AddHostedService<SampleConsole>();

               //Register projection IEntityTableClient<PersonEntity> named implementation factory to be injected with IAzureClientFactory<IEntityTableclient<PersonEntity>>
               services.AddAzureClients(clients =>
               {
                   clients.AddEntityBlobClient<DocumentEntity>(TestEnvironment.ConnectionString, builder =>
                   {
                       builder.ConfigureOptions(options =>
                      options.ContainerName = $"{nameof(DocumentEntity)}Container".ToLower());
                       builder.ConfigureEntity(config =>
                       {
                           config
                            .SetBlobContentProp(p => p.Content)
                            .SetBlobPath(p => $"{p.Created:yyyy/MM/dd}")
                            .SetBlobName(p => $"{p.Name}-{p.Reference}.{p.Extension}")
                            .AddTag(p => p.Reference)
                            .AddTag(p => p.Name);
                       });
                   }).WithName("DocumentEntityClient1");
               });
               services.AddEntityBlobClient<DocumentEntity>(TestEnvironment.ConnectionString, builder =>
                {

                    builder.ConfigureOptions(options =>
                     options.ContainerName= $"{nameof(DocumentEntity)}Container".ToLower());
                    builder.ConfigureEntity(config =>
                    {
                        config
                         .SetBlobContentProp(p => p.Content)
                         .SetBlobPath(p => $"{p.Created:yyyy/MM/dd}")
                         .SetBlobName(p => $"{p.Name}-{p.Reference}.{p.Extension}")
                         .AddTag(p => p.Reference)
                         .AddTag(p => p.Name);
                    });
                });
           });
    }
}