# Manage your entities in Azure Storage without tears !

## What is Azure Entity Services?

Azure Entity Services is an abstraction layer library which help you to **store**, **query**, **update** any entities classes in Azure Storage Table and Blob

 This library was written in C# Core and based on official **Azure SDK**

## Focused on performance and productity 

The library was designed to offer optimized performance and memory consumption to allow batched reading and writing operations on large entities store

Moreover, abstraction layers for Table and Blob will simplify implementation of your applications and provide simple and fast integration

**This will allow you to focus on you application business logic**


 
### What the meaning of Entity ?
An entity class is essentially an object wrapper for a database table.

The attributes of an entity are transformed to columns on the database table. 
Entities can have various data maintenance operations such as read, insert, modify, remove,
 readmulti (read multi reads multiple records from a table based on a partial key).

Standard operations such as read or insert operate on a single database table by default.


With this Library you can map any data entities (C# class) into a **table** or a **blob** item





This library is based on official [Azure.Data.Tables sdk library](https://devblogs.microsoft.com/azure-sdk/announcing-the-new-azure-data-tables-libraries/)

 
## EntityTableClient package features
 
* You can manage any generic C# entities without Azure SDK dependencies: no needs to inehrit from ITableEntity or TableEntity neither
* You can extend entity properties with dynamic properties (to simplify search and indexing nested objects)
* You can tag any entity or dynamic properties to be indexed for faster search for tables with large amount of items
* Handle more primitive types that are not supported by default in azure table storage 
* Lightweight and extensible query expression builder, used to build advanced filter expressions and indexed tags
* Observe entity changes, subscribe and apply any side effects like CQRS patterns
* It Could be injected and configured with Microsoft Azure extensions: AzureClientFactoryBuilder 

### How it works?

EntityTableClient bind any classes (C# entities) to Entity table storage (IEntityTableClient)

This binding allows to have more control when entity was stored of readed from the table storage

Internally, it use Azure storage ETG feature (entity transaction group) to keep indexed tag synchronized with the main entity.



### EntityTableClient configuration example

```csharp
            //==============Entity options and configuratin section====================================================
            //set here for your technical stuff: table name, connection, parallelization
            var entityClient = EntityTableClient.Create<PersonEntity>(
             options =>
            {
                options.ConnectionString = TestEnvironment.ConnectionString;
                options.TableName = $"{nameof(PersonEntity)}";
                options.CreateTableIfNotExists = true;
            }

            //set here your entity behavior dynamic fields, tags, observers
            , config =>
            {
                config
                .SetPartitionKey(p => p.TenantId)
                .SetPrimaryKeyProp(p => p.PersonId)
                .IgnoreProp(p => p.OtherAddress)

                //add tag to generate indexed and sorted entities through rowKey
                .AddTag(p => p.Created)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Distance)
                .AddTag(p => p.Enabled)

                //add computed props to store and compute dynamically additional fields of the entity
                .AddComputedProp("_IsInFrance", p => p.Address?.State == "France")
                .AddComputedProp("_MoreThanOneAddress", p => p.OtherAddress?.Count > 1)
                .AddComputedProp("_CreatedNext6Month", p => p.Created > DateTimeOffset.UtcNow.AddMonths(-6))
                .AddComputedProp("_FirstLastName3Chars", p => p.LastName?.ToLower()[..3])

                //computed props could also be tagged 
                .AddTag("_FirstLastName3Chars")

                //add an entity oberver to track entity changes and apply any action (projection, logging, etc.)
                .AddObserver("EntityLoggerObserver", new EntityLoggerObserver<PersonEntity>());
            });
            //===============================================================================================

```


### Output of sample console projet based on a table with 1.5 million of entities and only 5 partitions (standard storageV2)
 
![image](https://user-images.githubusercontent.com/4396827/213818315-bf0370d3-82f2-4908-b969-761bd0b3b9de.png)


### Same table with added 10K entities and parallelization setted up to 64 concurrent batch operations

![image](https://user-images.githubusercontent.com/4396827/213819426-3c9d2896-07db-4601-8355-b36c22440235.png)


### Added an implementation of entity observer

![image](https://user-images.githubusercontent.com/4396827/213823101-c36917fe-93a1-4fef-bf14-6b363e9eb32b.png)


## EntityBlobClient package features
 
* Like EntityTableClient, you can manage any generic C# entities without Azure SDK dependencies
* Entities properties was mapped and stored directly into Blob metadata, no need to maintain a relation with blob and any additional tables 
* You can tag any entity or dynamic properties to be indexed natively by Azure Blob Storage service
* It use also same query expression builder of EntityTableClient to query Blobs inside a container
* It could be injected and configured with Microsoft Azure extensions: AzureClientFactoryBuilder 
* Abstract and simplify Blob reference path by using dynamic path delegate based on your entity properties (SetBlobPath and SetBlobName)





### EntityTableClient configuration example

```csharp
     var options = new EntityBlobClientOptions($"{nameof(DocumentEntity)}Container".ToLower());

            //Configure entity binding in the table storage
            var client = EntityBlobClient.Create<DocumentEntity>(TestEnvironment.ConnectionString)
                .Configure(options, config =>
             config
                .SetBlobContentProp(p => p.Content)
                .SetBlobPath(p => $"{p.Created:yyyy/MM/dd}")
                .SetBlobName(p => $"{p.Name}-{p.Reference}.{p.Extension}")
                .AddTag(p => p.Reference)
                .AddTag(p => p.Name));
```
Upcoming:
* Expand test coverage
* Add more validation rules according to [azure storage limitations](https://docs.microsoft.com/en-us/azure/azure-resource-manager/management/azure-subscription-service-limits#azure-table-storage-limits)
* Gradually improvement of EntityBlobClient  
* More description of the internal implementation of this library
* Entity migration services, usefull for data or structural migration
