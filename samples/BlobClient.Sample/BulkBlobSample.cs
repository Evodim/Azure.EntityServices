using Azure.EntityServices.Blobs;
using Bogus;
using Common.Samples;
using Common.Samples.Diagnostics;
using Common.Samples.Models;
using Common.Samples.Tools;
using Common.Samples.Tools.Fakes;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BlobClient.BasicSample
{

    
    public static class BulkBlobSample
    {
        private const int ENTITY_COUNT = 50;

        

        public static async Task Run()
        {
            var options = new EntityBlobClientOptions($"{nameof(CountryRoadsEntity)}Container".ToLower());

            //Configure entity binding in the table storage
            var client = EntityBlobClient.Create<CountryRoadsEntity>(TestEnvironment.ConnectionString)
                .Configure(options, config =>
                  
             config
                .SetBlobPath(p=>"")
                .SetBlobContentProp(entity => entity.Roads)
                .SetBlobName(entity => $"{entity.CountryCode.ToUpperInvariant()}-roads.json"));

            
            var entities = new List<CountryRoadsEntity>() { new CountryRoadsEntity() {
                 CountryCode= "FR2",
                 Roads= new List<RoadItem>() { new RoadItem() { 
                     type ="Feature",
                     geometry=new(),
                     properties=new()} }

            } };

            Console.WriteLine("OK");

            var counters = new PerfCounters(nameof(EntityBlobClient<PersonEntity>));
            Console.Write($"Insert {ENTITY_COUNT} entities...");

            using (var mesure = counters.Mesure($"{ENTITY_COUNT} insertions"))
            {
                foreach (var entity in entities)
                {
                    await client.AddOrReplaceAsync(entity);
                }
            }
            using (var mesure = counters.Mesure($"readed"))
            {
                var count = 0;
                await foreach (var readed in client.ListAsync(""))
                {
                    foreach (var entity in readed)
                    {
                     Console.WriteLine(entity.CountryCode);
                        count++;
                    }
                }
                Console.WriteLine($"Readed : {count}");
            }

            Console.WriteLine("====================================");
            counters.WriteToConsole();
        }
    }
}