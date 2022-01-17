# Azure.EntityServices
Services to manage pure and generic entities in Azure blobs and tables


## Azure.EntityServices is a set of services for azure storage services
Initial project (experimental) was localted here: [EntityStorageServices](https://github.com/Evodim/EntityStorageServices)

This new version was partiallly rewritted and based on the new official [Azure.Data.Tables library](https://devblogs.microsoft.com/azure-sdk/announcing-the-new-azure-data-tables-libraries/)

Azure.EntityServices help you to store, update and search pure and generic entities in Azure table storage
Pure entities could be any classes without dependencies on any framework or azure storage implementation



This project is focused on entities abstraction and performance
 
Features:

* You can use any pure and generic entities without azure sdk dependencies: no need to inehrits from ITableEntity or TableEntity neither
* You can extend entity properties with dynamic properties (and could be tagged)
* You can tag any entity or dynamic properties to be indexed for faster search in large amount of items
* Handle more primitive types that are not supported by default in azure table storage 
* Lightweight and extensible query expression builder (used to build query filter expressions)
* Entity table observers, subscribe and apply side effects when any entity changed (experimental)
 
### How it works?

EntityTableClient bind any classes (entities) to Entity table storage
This binding allows to have more control when entity was stored of readed from the table storage
Internally, it use Azure storage ETG feature (entity transaction group) to keep indexed tag synchronized with the main entity.

Upcoming:
* Expand test coverage
* Add validation rules according to [azure storage limitations](https://docs.microsoft.com/en-us/azure/azure-resource-manager/management/azure-subscription-service-limits#azure-table-storage-limits)
* Publish EntityBlobClient  
* More description of the internal implementation of this library
* Entity migration services, usefull for data or structural migration

### EntityTableClient configuration example

```csharp
  
 var entityClient = new EntityTableClient<PersonEntity>(options, config =>
            {
                config
                .SetPartitionKey(p => p.TentantId)
                .SetPrimaryProp(p => p.PersonId)
                .AddTag(p => p.Created)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Distance)
                .AddTag(p => p.Enabled)
                .AddTag(p => p.Latitude)
                .AddTag(p => p.Longitude)

                .AddComputedProp("_IsInFrance", p => p.Address.State == "France")
                .AddComputedProp("_MoreThanOneAddress", p => p.OtherAddress.Count > 1)
                .AddComputedProp("_FirstLastName3Chars", p => p.LastName.ToLower()[..3])

                .AddTag("_FirstLastName3Chars");
            });

```


### Output of sample console projet based on a table with 3 billions of entities (standard storageV2)

```
====================================
Generate faked 2000 entities...Ok
Insert 4000 entities...in 12,6176517 seconds
Querying entities 1 times...
====================================
1. Get By Id 0,037 seconds
2. Get By LastName 14,275 seconds
3. Get By LastName (indexed tag) 0,211 seconds
4. Get LastName start with 'arm' 8,29 seconds
5. Get by LastName start with 'arm' (indexed tag) 0,776 seconds```

*You should use a real azure table storage connection with more than 100K entities to highlight performance improvment with indexed tags*

