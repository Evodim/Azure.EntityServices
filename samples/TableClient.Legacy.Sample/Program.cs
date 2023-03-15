using Azure.EntityServices.Tables;
using Azure.EntityServices.Tables.Extensions.DependencyInjection;
using Common.Samples;
using Common.Samples.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;

namespace TableClient.LegacySample
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
               services.AddEntityTableClient<PersonEntity>(TestEnvironment.ConnectionString, builder =>
               {
                   builder
                   .ConfigureOptions(options =>
                   {
                       options.TableName = $"{nameof(PersonEntity)}";
                       options.CreateTableIfNotExists = true;
                   })
                   .ConfigureEntity(entityConfig => entityConfig
                      .SetPartitionKey(entity => entity.TenantId)
                      .SetRowKeyProp(entity => entity.PersonId)

                      .IgnoreProp(entity => entity.OtherAddress)

                      .AddComputedProp("_IsInFrance", entity => entity.Address?.State == "France")
                      .AddComputedProp("_MoreThanOneAddress", entity => entity.OtherAddress?.Count > 1)
                      .AddComputedProp("_CreatedNext6Month", entity => entity.Created > DateTimeOffset.UtcNow.AddMonths(-6))
                      .AddComputedProp("_FirstLastName3Chars", entity => entity.LastName?.ToLower()[..3])

                      .AddTag(entity => entity.Created)
                      .AddTag(entity => entity.LastName)
                      .AddTag("_FirstLastName3Chars"));
               });

               services.AddHostedService<SampleConsole>();
           });
    }
}