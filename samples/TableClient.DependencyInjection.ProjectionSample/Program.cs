using Azure.EntityServices.Tables;
using Azure.EntityServices.Tables.Extensions.DependencyInjection;
using Common.Samples;
using Common.Samples.Models;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace TableClient.DependencyInjection.ProjectionSample
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
                   }))
                   .WithName("Projection");

                   clients
                   .AddEntityTableClient<PersonEntity>(TestEnvironment.ConnectionString,
                   entityBuilder => entityBuilder
                   .ConfigureOptions(options =>
                   {
                       options.TableName = $"{nameof(PersonEntity)}";
                       options.CreateTableIfNotExists = true;
                   })
                   .ConfigureEntity((sp, config) => config
                   .SetPartitionKey(p => p.TenantId)
                   .SetRowKeyProp(p => p.PersonId)
                   .AddTag(p => p.LastName)
                   .IgnoreProp(p => p.OtherAddress)
                   .AddComputedProp("_IsInFrance", p => p.Address?.State == "France")
                   .AddComputedProp("_MoreThanOneAddress", p => p.OtherAddress?.Count > 1)
                   ))
                   .WithName("Source");
               });

           });
    }
}