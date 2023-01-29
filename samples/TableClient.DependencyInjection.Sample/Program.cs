using Azure.EntityServices.Tables;
using Azure.EntityServices.Tables.Extensions;
using Azure.EntityServices.Tables.Extensions.DependencyInjection;
using Common.Samples;
using Common.Samples.Models;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using System;

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
               var tableClientOptions = new EntityTableClientOptions(
             TestEnvironment.ConnectionString,
             $"{nameof(PersonEntity)}",
             createTableIfNotExists: true);

               var projectionClientOptions = new EntityTableClientOptions(
                tableClientOptions.ConnectionString,
                tableClientOptions.TableName);

               services.AddTransient(sp => new SampleProjectionObserver().Configure(projectionClientOptions));
             
               services.AddEntityTableClient<PersonEntity>(tableClientOptions, configBuilder =>
               { 
                   configBuilder
                      .SetPartitionKey(p => p.TenantId)
                      .SetRowKeyProp(p => p.PersonId)

                      .IgnoreProp(p => p.OtherAddress)

                      .AddComputedProp("_IsInFrance", p => p.Address?.State == "France")
                      .AddComputedProp("_MoreThanOneAddress", p => p.OtherAddress?.Count > 1)
                      .AddComputedProp("_CreatedNext6Month", p => p.Created > DateTimeOffset.UtcNow.AddMonths(-6))
                      .AddComputedProp("_FirstLastName3Chars", p => p.LastName?.ToLower()[..3])

                      .AddTag(p => p.Created)
                      .AddTag(p => p.LastName)
                      .AddTag("_FirstLastName3Chars");

                   configBuilder.AddObserver<PersonEntity, SampleProjectionObserver>("LastNameProjection");
               });
               services.AddHostedService<EntityTableClientSampleConsole>();
           });
    }
}