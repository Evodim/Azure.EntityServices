﻿using Azure.EntityServices.Queries;
using Azure.EntityServices.Queries.Core;

namespace Azure.EntityServices.Tables.Extensions
{

    /// <summary>
    /// Helpers to extend filter expression with partition key et row key filters abstraction
    /// </summary>
    public static class IQueryComposeExtensions
    {
        public static IQueryFilter<T> WherePartitionKey<T>(this IQueryCompose<T> query) => query.AddQuery("PartitionKey");

        public static IQueryFilter<T> WhereRowKey<T>(this IQueryCompose<T> query) => query.AddQuery("RowKey");

        public static IQueryFilter<T> AndRowKey<T>(this IFilterOperator<T> query) => query.AddOperator(nameof(IQueryInstructions.And), "RowKey");

        public static IQueryFilter<T> NotRowKey<T>(this IFilterOperator<T> query) => query.AddOperator(nameof(IQueryInstructions.Not), "RowKey");

        public static IQueryFilter<T> OrRowKey<T>(this IFilterOperator<T> query) => query.AddOperator(nameof(IQueryInstructions.Or), "RowKey");

        public static IQueryFilter<T> AndPartitionKey<T>(this IFilterOperator<T> query) => query.AddOperator(nameof(IQueryInstructions.And), "PartitionKey");

        public static IQueryFilter<T> NotPartitionKey<T>(this IFilterOperator<T> query) => query.AddOperator(nameof(IQueryInstructions.Not), "PartitionKey");

        public static IQueryFilter<T> OrPartitionKey<T>(this IFilterOperator<T> query) => query.AddOperator(nameof(IQueryInstructions.Or), "PartitionKey");
    }

}