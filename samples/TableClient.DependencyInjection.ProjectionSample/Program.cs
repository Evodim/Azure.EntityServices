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
                   .SetPartitionKey(entity => $"~projection-{entity.LastName?.ToLowerInvariant()[..3]}")
                   .SetRowKey(entity => $"{entity.LastName}-{entity.PersonId}"))
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
                   .SetPartitionKey(entity => entity.TenantId)
                   .SetRowKeyProp(entity => entity.PersonId)
                   .AddTag(entity => entity.LastName)
                   .IgnoreProp(entity => entity.OtherAddresses)
                   .AddComputedProp("_IsInFrance", entity => entity.Address?.State == "France")
                   .AddComputedProp("_MoreThanOneAddress", entity => entity.OtherAddresses?.Count > 1)
                   ))
                   .WithName("Source");
               });

           });
    }
}