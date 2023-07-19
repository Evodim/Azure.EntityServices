using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables;
using System;
using System.Linq;
using System.Threading.Tasks;
using Common.Samples.Models;
using Common.Samples.Diagnostics;
using Common.Samples.Tools;
using Common.Samples;
using Common.Samples.Tools.Fakes;
using Azure.Data.Tables;

namespace TableClient.PerformanceSample
{
    public static class SampleConsole
    {
        private const int ENTITY_COUNT = 200;

        public static async Task Run()
        {
            //==============Entity options and configuratin section====================================================
            //set here for your technical stuff: table name, connection, parallelization
            var entityClient = EntityTableClient.Create<TableEntity>(TestEnvironment.ConnectionString)
            .Configure(options =>
            {
                options.TableName = "UserSearchProjection";
                options.CreateTableIfNotExists = true;
            }

            //set here your entity behavior dynamic fields, tags, observers
            , config =>
            {
                config
                .SetPartitionKey(entity => entity.PartitionKey)
                .SetRowKeyProp(entity => entity.RowKey);
             
            });
            //===============================================================================================

            //==============Entity options and configuratin section====================================================
            //set here for your technical stuff: table name, connection, parallelization
            var entityClient2 = EntityTableClient.Create<TableEntity>(TestEnvironment.ConnectionString)
            .Configure(options =>
            {
                options.TableName = "UserSearchProjectionV2";
                options.CreateTableIfNotExists = true;
                options.MaxItemToGroup = 10000;
            }

            //set here your entity behavior dynamic fields, tags, observers
            , config =>
            {
                config
                .SetPartitionKey(entity => $"{entity.PartitionKey}-{(entity["ExternalId"] as string)?[..3] ?? "_null"}" )
                .SetRowKeyProp(entity => entity.RowKey);

            });


           await foreach(var entity in entityClient.GetAsync()) { 
              
             await entityClient2.AddManyAsync(entity.OrderBy(o => o["ExternalId"]));
            }
      
        }
    }
}