using Azure.EntityServices.Tables;
using Azure.EntityServices.Tables.Extensions;
using Azure.EntityServices.Tables.Extensions.DependencyInjection;
using Common.Samples;
using Common.Samples.Models;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace TableClient.DependencyInjection.Sample
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
               services.AddHostedService<ProjectionWithDependencyInjectionSampleConsole>();

               services
               .AddTransient<SampleProjectionObserver>();

               //Register projection IEntityTableClient<PersonEntity> named implementation factory to be injected with IAzureClientFactory<IEntityTableclient<PersonEntity>>
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

               //Register default IEntityTableClient<PersonEntity> implementation factory to be injected directly with IEntityTableclient<PersonEntity>
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