# Azure.EntityServices
## Azure Storage client to use generic entities in tables and blobs
![Public](https://github.com/evodim/Azure.EntityServices/actions/workflows/publish-public.yml/badge.svg)
![Internal](https://github.com/evodim/Azure.EntityServices/actions/workflows/publish-internal.yml/badge.svg)


## Purpose

Azure.EntityServices help you to store, update and search generic entities in Azure storage tables and blobs
Entities could be any pure classes without any infrastructure specific dependencies

Initial project (experimental) was located here: [EntityStorageServices](https://github.com/Evodim/EntityStorageServices)
This new version was partiallly rewritted and based on the new official [Azure.Data.Tables sdk library](https://devblogs.microsoft.com/azure-sdk/announcing-the-new-azure-data-tables-libraries/)

This project is focused on azure sdk abstraction and performance
 
Features:

* You can use any generic entities without azure sdk dependencies: no needs to inehrit from ITableEntity or TableEntity neither
* You can extend entity properties with dynamic properties 
* You can tag any entity or dynamic properties to be indexed for faster search for tables with large amount of items
* Handle more primitive types that are not supported by default in azure table storage 
* Lightweight and extensible query expression builder (used to build advanced filter expressions)
* Entity table observers, subscribe and apply side effects when any entity changed (experimental)
 
## How it works?

EntityTableClient bind any classes (entities) to Entity table storage
This binding allows to have more control when entity was stored of readed from the table storage
Internally, it use Azure storage ETG feature (entity transaction group) to keep indexed tag synchronized with the main entity.

Upcoming:
* Expand test coverage
* Add validation rules according to [azure storage limitations](https://docs.microsoft.com/en-us/azure/azure-resource-manager/management/azure-subscription-service-limits#azure-table-storage-limits)
* Gradually improvement of EntityBlobClient  
* More description of the internal implementation of this library
* Entity migration services, usefull for data or structural migration


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
