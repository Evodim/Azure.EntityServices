using Azure.Data.Tables;

namespace Azure.EntityServices.Tables
{
    internal static class RequestFailedExtensions
    {
        internal static bool HandleAzureStorageException(this RequestFailedException requestFailedException, string tableName, TableServiceClient tableService, bool createTableIfNotExists)
        {
            try
            {
                if (createTableIfNotExists && (requestFailedException?.ErrorCode == "TableNotFound"))
                {
                    tableService.CreateTableIfNotExists(tableName);
                    return true;
                }

                if (requestFailedException?.ErrorCode == "TableBeingDeleted" ||
                    requestFailedException?.ErrorCode == "OperationTimedOut" ||
                    requestFailedException?.ErrorCode == "TooManyRequests"
                    )
                {
                    return true;
                }
            }
            catch (RequestFailedException ex)
            {
                if (ex?.ErrorCode == "TableBeingDeleted" ||
                   ex?.ErrorCode == "OperationTimedOut" ||
                   ex?.ErrorCode == "TooManyRequests"
                   )
                {
                    return true;
                }
            }
            return false;
        }

    }
}