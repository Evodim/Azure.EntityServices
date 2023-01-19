using Azure.Data.Tables;
using System;

namespace Azure.EntityServices.Tables.Extensions
{
    internal static class TableTransactionActionTypeExtensions
    {
        public static EntityOperation MapToEntityOperation(this TableTransactionActionType tableTransactionActionType)
        { 
            return tableTransactionActionType switch
            {
                TableTransactionActionType.Add => EntityOperation.Add,
                TableTransactionActionType.UpdateMerge => EntityOperation.Merge,
                TableTransactionActionType.UpdateReplace => EntityOperation.Replace,
                TableTransactionActionType.Delete => EntityOperation.Delete,
                TableTransactionActionType.UpsertMerge => EntityOperation.AddOrMerge,
                TableTransactionActionType.UpsertReplace => EntityOperation.AddOrReplace,
                _ => throw new NotSupportedException($"TableTransactionActionType {tableTransactionActionType} not supported by Azure EntityServices")
            };
        }
    }
}