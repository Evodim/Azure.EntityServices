using Azure.Data.Tables;
using System.Collections.Generic;

namespace Azure.EntityServices.Tables.Core
{
    public class EntityTransactionGroup
    {
        public EntityTransactionGroup(string partitionKey)
        {
            PartitionKey = partitionKey;
           
        } 
        public string PartitionKey { get; private set; }
        public List<TableTransactionAction> Actions { get; set; } = new List<TableTransactionAction>();
    }
}