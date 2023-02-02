using Azure.EntityServices.Tables;
using Azure.EntityServices.Tables.Extensions;
using Azure.EntityServices.Tables.Extensions.DependencyInjection;
using Common.Samples;
using Common.Samples.Models;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.DependencyInjection;
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
               services.AddHostedService<EntityTableClientSampleConsole>();

               var tableClientOptions = new EntityTableClientOptions(
               $"{nameof(PersonEntity)}",
               createTableIfNotExists: true);

               var projectionClientOptions = new EntityTableClientOptions(
                tableClientOptions.TableName);

               services
               .AddTransient(sp => new SampleProjectionObserver()
                                    .Configure(TestEnvironment.ConnectionString, projectionClientOptions));

               //Register named  IEntityTableClient<TEntity> implementation factories using AzureClientFactoryBuilder
               services.AddAzureClients(clients =>
               {
                   clients
                       .AddEntityTableClient<PersonEntity>(TestEnvironment.ConnectionString,
                       entityBuilder => entityBuilder 
                       .ConfigureEntity(config =>
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
                            .AddTag("_FirstLastName3Chars")))

                       .ConfigureOptions(options =>
                       {
                           options.TableName = $"{nameof(PersonEntity)}1";
                           options.CreateTableIfNotExists = true;
                       })
                       .WithName($"{nameof(PersonEntity)}1");

                   clients
                       .AddEntityTableClient<PersonEntity>(TestEnvironment.ConnectionString,
                        entityBuilder => entityBuilder
                          .ConfigureEntity(config => config
                            .SetPartitionKey(p => p.TenantId)
                            .SetRowKeyProp(p => p.PersonId)
                            .IgnoreProp(p => p.OtherAddress)
                            .AddComputedProp("_IsInFrance", p => p.Address?.State == "France")
                            .AddComputedProp("_MoreThanOneAddress", p => p.OtherAddress?.Count > 1)
                            .AddComputedProp("_CreatedNext6Month", p => p.Created > DateTimeOffset.UtcNow.AddMonths(-6))
                            .AddComputedProp("_FirstLastName3Chars", p => p.LastName?.ToLower()[..3])
                            .AddTag(p => p.Created)
                            .AddTag(p => p.LastName)
                            .AddTag("_FirstLastName3Chars")))

                          .ConfigureOptions(options =>
                          {
                              options.TableName = $"{nameof(PersonEntity)}2";
                              options.CreateTableIfNotExists = true;
                          })
                          .WithName($"{nameof(PersonEntity)}2");
               });

               //default/global IEntityTableClient<TEntity> registration (without AzureClientFactoryBuilder)
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
                      .AddComputedProp("_CreatedNext6Month", p => p.Created > DateTimeOffset.UtcNow.AddMonths(-6))
                      .AddComputedProp("_FirstLastName3Chars", p => p.LastName?.ToLower()[..3])
                      .AddTag(p => p.Created)
                      .AddTag(p => p.LastName)
                      .AddTag("_FirstLastName3Chars")
                      .AddObserver("LastNameProjection", () => sp.GetService<SampleProjectionObserver>())
                      );
               });
           });
    }
}