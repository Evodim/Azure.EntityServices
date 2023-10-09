using Azure.EntityServices.Blobs;
using Bogus;
using Common.Samples;
using Common.Samples.Diagnostics;
using Common.Samples.Models;
using Common.Samples.Tools;
using Common.Samples.Tools.Fakes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BlobClient.BasicSample
{

    
    public static class BulkBlobSample
    {
        private const int ENTITY_COUNT = 10;
               

        public static async Task Run()
        {
            var counters = new PerfCounters(nameof(EntityBlobClient<PersonEntity>));

            var options = new EntityBlobClientOptions($"{nameof(CountryRoadsEntity)}Container".ToLower());

            //Configure entity binding in the table storage
            var client = EntityBlobClient.Create<CountryRoadsEntity>(TestEnvironment.ConnectionString)
                .Configure(options, config =>
                  
             config
                .SetBlobPath(p=>"")
                .SetBlobContentProp(entity => entity.Roads) 
                .SetBlobName(entity => $"{entity.CountryCode?.ToUpperInvariant()}-roads.json"));

            
            var entities = 
                Enumerable.Repeat<int>(0, ENTITY_COUNT).Select(e=> new CountryRoadsEntity() {
                 CountryCode= Guid.NewGuid().ToString(),
                 Roads= new List<RoadItem>() { new RoadItem() { 
                     type ="Feature",
                     geometry=new(),
                     properties=new()} }

            });
             
          
            //using (var mesure = counters.Mesure($"Added"))
            //{
            //    var count = 0;
            //    foreach (var entity in entities)
            //    {
                    
            //      await client.AddOrReplaceAsync(entity);
            //       count++;
            //    }
            //    Console.WriteLine($"added : {count}");
            //}
            using (var mesure = counters.Mesure($"Readed"))
            {
                var count = 0;
                await foreach (var readed in client.ListPropsAsync(""))
                {
                    var entity = new CountryRoadsEntity()
                    {
                        CountryCode = readed["Countrycode"] ,
                        RoadCount = 0
                    };

                }

                    await foreach (var readed in client.ListAsync(""))
                { 
                    foreach (var entity in readed)
                    { 
                        var content = await client.GetContentAsync(entity);
                        using (var stream = content.ToStream())
                        {
                            long roadCount = 0;
                            stream.DeserializeItems<RoadItem>(r =>
                            {
                                roadCount++;
                            });
                            entity.RoadCount = roadCount;
                            await client.AddOrReplaceAsync(entity);
                        }
                        count++;
                    }
                }
                Console.WriteLine($"Readed : {count}");
            }

            using (var mesure = counters.Mesure($"updated"))
            {
                var count = 0;

                await foreach (var readed in client.ListAsync(""))
                {

                    foreach (var entity in readed)
                    {

                        await client.AddOrReplaceAsync(entity);
                        count++;
                    }
                }
                Console.WriteLine($"Updated : {count}");
            }

            Console.WriteLine("====================================");
            counters.WriteToConsole();
        }
    }
}