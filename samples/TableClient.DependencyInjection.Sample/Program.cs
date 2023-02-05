using Azure.EntityServices.Tables;
using Azure.EntityServices.Tables.Extensions;
using Azure.EntityServices.Tables.Extensions.DependencyInjection;
using Common.Samples;
using Common.Samples.Models;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace TableClient.DependencyInjectionSample
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

               services
               .AddTransient<SampleProjectionObserver>();

               //IEntityTableClient<T> could be registred in two ways:

               //Register IAzureClientFactory<IEntityTableClient<T>> to inject IEntityTableClient<T> named implementation factory
               services.AddAzureClients(clients =>
               {
                   clients
                       .AddEntityTableClient<PersonEntity>(TestEnvironment.ConnectionString,
                        entityBuilder => entityBuilder
                          .ConfigureEntity(config => config
                            .SetPartitionKey(p => $"~projection-{p.LastName?.ToLowerInvariant()[..3]}")
                            .SetRowKey(p => $"{p.LastName}-{p.PersonId}"))
                          .ConfigureOptions(options =>
                            {
                                options.TableName = $"{nameof(PersonEntity)}";
                                options.CreateTableIfNotExists = true;
                            }))
                          .WithName($"{nameof(SampleProjectionObserver)}");
               });

               //Register directly IEntityTableClient<T> as a global and default injection
               services.AddEntityTableClient<PersonEntity>(TestEnvironment.ConnectionString, builder =>
               {
                   builder
                     .ConfigureOptions(options =>
                   {
                       options.TableName = $"{nameof(PersonEntity)}";
                       options.CreateTableIfNotExists = true;
                   })
                   .ConfigureEntity((sp, config) => config
                      .SetPartitionKey(p => p.TenantId)
                      .SetRowKeyProp(p => p.PersonId)
                      .IgnoreProp(p => p.OtherAddress)
                      .AddComputedProp("_IsInFrance", p => p.Address?.State == "France")
                      .AddComputedProp("_MoreThanOneAddress", p => p.OtherAddress?.Count > 1)
                      .AddObserver("LastNameProjection", () => sp.GetService<SampleProjectionObserver>())
                      );
               });
           });
    }
}