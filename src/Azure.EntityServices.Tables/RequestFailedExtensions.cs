using Azure.Data.Tables;
using System.Linq;

namespace Azure.EntityServices.Tables
{
    internal static class RequestFailedExtensions
    {
        internal static string[] HandledErrorCodes = new string[] {
            "TableBeingDeleted",
            "OperationTimedOut",
            "TooManyRequests" };

        internal static bool HandleAzureStorageException(
            this RequestFailedException requestFailedException,
            string tableName,
            TableServiceClient tableService,
            bool createTableIfNotExists)
        {
            try
            {
                if (createTableIfNotExists && (requestFailedException?.ErrorCode == "TableNotFound"))
                {
                    tableService.CreateTableIfNotExists(tableName);
                    return true;
                }

                if (HandledErrorCodes.Contains(requestFailedException?.ErrorCode))
                {
                    return true;
                }
            }
            catch (RequestFailedException ex)
            {
                if (HandledErrorCodes.Contains(ex?.ErrorCode))
                {
                    return true;
                }
            }
            return false;
        }

    }
}