using Azure.Data.Tables;
using System;

namespace Azure.EntityServices.Tables.Extensions
{
    internal static class TableTransactionActionTypeExtensions
    {
        public static EntityOperationType MapToEntityOperation(this TableTransactionActionType tableTransactionActionType)
        { 
            return tableTransactionActionType switch
            {
                TableTransactionActionType.Add => EntityOperationType.Add,
                TableTransactionActionType.UpdateMerge => EntityOperationType.Merge,
                TableTransactionActionType.UpdateReplace => EntityOperationType.Replace,
                TableTransactionActionType.Delete => EntityOperationType.Delete,
                TableTransactionActionType.UpsertMerge => EntityOperationType.AddOrMerge,
                TableTransactionActionType.UpsertReplace => EntityOperationType.AddOrReplace,
                _ => throw new NotSupportedException($"TableTransactionActionType {tableTransactionActionType} not supported by Azure EntityServices")
            };
        }
        public static TableTransactionActionType MapToTableTransactionActionType(this EntityOperationType entityOperation)
        {
            return entityOperation switch
            {
                EntityOperationType.Add => TableTransactionActionType.Add,
                EntityOperationType.Merge => TableTransactionActionType.UpdateMerge,
                EntityOperationType.Replace => TableTransactionActionType.UpdateReplace,
                EntityOperationType.Delete => TableTransactionActionType.Delete,
                EntityOperationType.AddOrMerge => TableTransactionActionType.UpsertMerge,
                EntityOperationType.AddOrReplace => TableTransactionActionType.UpsertReplace,
                _ => throw new NotSupportedException($"EntityOperation {entityOperation} not supported by Azure EntityServices")
            };
        }
    }
}