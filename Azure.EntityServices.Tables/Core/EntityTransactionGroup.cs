using Azure.Data.Tables;
using System.Collections.Generic;

namespace Azure.EntityServices.Tables.Core
{
    public class EntityTransactionGroup
    {
        public EntityTransactionGroup(string partitionKey, string primaryKey)
        {
            PartitionKey = partitionKey;
            PrimaryKey = primaryKey;
        }

        public string PrimaryKey { get; private set; }
        public string PartitionKey { get; private set; }
        public List<TableTransactionAction> Actions { get; set; } = new List<TableTransactionAction>();
    }
}