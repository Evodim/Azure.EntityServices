using System;

namespace Azure.EntityServices.Tables.Core
{
    public static class TableConstants
    {
        public const int TableServiceBatchMaximumOperations = 100;
        public const string Select = "$select";
        public const string Top = "$top";
        public const string Filter = "$filter";
        public const string TableName = "TableName";
        public const string Etag = "ETag";
        public const string Timestamp = "Timestamp";
        public const string RowKey = "RowKey";
        public const string PartitionKey = "PartitionKey";
        public const string TableServiceTablesName = "Tables";
        public const int TableServiceMaxStringPropertySizeInChars = 32768;
        public const long TableServiceMaxPayload = 20971520;
        public const int TableServiceMaxStringPropertySizeInBytes = 65536;
        public const int TableServiceMaxResults = 1000;
        public const string TableServiceNextTableName = "NextTableName";
        public const string TableServiceNextRowKey = "NextRowKey";
        public const string TableServiceNextPartitionKey = "NextPartitionKey";
        public const string TableServicePrefixForTableContinuation = "x-ms-continuation-";
        public const string UserAgentProductVersion = "1.0.6";
        public static DateTime DateTimeStorageDefault => 
            new DateTime(1601, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    }
}