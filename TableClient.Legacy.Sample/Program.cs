﻿using System; 
using Azure.EntityServices.Tables;
using Azure.EntityServices.Tables.Extensions;
using Common.Samples;
using Common.Samples.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;


namespace TableClient.Legacy.Sample
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

               services.AddEntityTableClient<PersonEntity>(tableClientOptions, config =>
               {
                   config
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
                });

               services.AddHostedService<LegacySample>();
           });
    }
}