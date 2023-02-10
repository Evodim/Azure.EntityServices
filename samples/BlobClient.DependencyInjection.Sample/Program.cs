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

               //IEntityBlobClient<T> could be registred in two ways:

               //Register IAzureClientFactory<IEntityBlobClient<T>> to inject IEntityBlobClient<T> named implementation factory
               services.AddAzureClients(clients =>
               {
                   clients.AddEntityBlobClient<DocumentEntity>(TestEnvironment.ConnectionString, builder =>
                   {
                       builder.ConfigureOptions(options =>
                      options.ContainerName = $"{nameof(DocumentEntity)}Container".ToLower());
                       builder.ConfigureEntity(config =>
                       {
                           config
                            .SetBlobContentProp(entity => entity.Content)
                            .SetBlobPath(entity => $"{entity.Created:yyyy/MM/dd}")
                            .SetBlobName(entity => $"{entity.Name}-{entity.Reference}.{entity.Extension}")
                            .AddTag(p => p.Reference)
                            .AddTag(p => p.Name);
                       });
                   }).WithName("DocumentEntityClient1");
               });

               //Or
               //Register directly IEntityBlobClient<T> as a global and default injection
               services.AddEntityBlobClient<DocumentEntity>(TestEnvironment.ConnectionString, builder =>
                {

                    builder.ConfigureOptions(options =>
                     options.ContainerName= $"{nameof(DocumentEntity)}Container".ToLower());
                    builder.ConfigureEntity(config =>
                    {
                        config
                         .SetBlobContentProp(p => p.Content)
                         .SetBlobPath(entity => $"{entity.Created:yyyy/MM/dd}")
                         .SetBlobName(entity => $"{entity.Name}-{entity.Reference}.{entity.Extension}")
                         .AddTag(entity => entity.Reference)
                         .AddTag(entity => entity.Name);
                    });
                });
           });
    }
}