using Azure.EntityServices.Tables;
using Azure.EntityServices.Tables.Extensions.DependencyInjection;
using Common.Samples;
using Common.Samples.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace TableClient.DependencyInjection.BasicSample
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
                      .SetPartitionKey(entity => entity.TenantId)
                      .SetRowKeyProp(entity => entity.PersonId)
                      .IgnoreProp(entity => entity.OtherAddresses)
                      .AddComputedProp("_IsInFrance", entity => entity.Address?.State == "France")
                      .AddComputedProp("_MoreThanOneAddress", entity => entity.OtherAddresses?.Count > 1) 
                      );
               });
           });
    }
}