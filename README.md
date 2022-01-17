
## Azure.EntityServices is a set of extensions for azure storage services.
Initial project (experimental) was called: EntityTableService.
This new version was partiallly rewritted and based on the new Azure.Data.Tables library

Azure.EntityServices help you to store, update and search pure entities in Azure table storage.
Pure entities are any classes without dependencies on some framework or azure storage implementation



This project is focused on entities abstraction and performance.
 
Features:

* You can use any pure and generic entities without azure sdk dependencies: no need to inehrits from ITableEntity or TableEntity neither.
* You can extend entity properties with dynamic props  
* You can tag any entity or dynamic props to be indexed for fast search in large amount of data
* Lightweight and extensible query expression builder (used to build query filter expressions)
* Entity table observers, subscribe and apply side effects when any entity changed

Upcomming:
* Entity migration services: bulk update or delete to rehydrate or update entities massively

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


