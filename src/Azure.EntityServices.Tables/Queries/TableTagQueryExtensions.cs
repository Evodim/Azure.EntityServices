﻿using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables.Extensions;
using System;
using System.Linq.Expressions;

namespace Azure.EntityServices.Tables
{
    /// <summary>
    /// Extend filter expression with partition key et row key filters abstraction
    /// </summary>
    public static class TableTagQueryExtensions
    {

        public static ITagQueryFilter<T> WhereTag<T>(this IQuery<T> query) =>

            (query as ITagQueryCompose<T>).AddQuery("RowKey");

        public static ITagQueryFilter<T> WhereTag<T>(this IQuery<T> query, string tagName)
        {
            (query as ITagQueryCompose<T>).TagName = tagName;
            return (query as ITagQueryCompose<T>).AddQuery("RowKey");
        }

        public static ITagQueryFilter<T> WhereTag<T, P>(this IQuery<T> query, Expression<Func<T, P>> tagSelector)
        {
            (query as ITagQueryCompose<T>).TagName = tagSelector.GetPropertyInfo().Name;
            return (query as ITagQueryCompose<T>).AddQuery("RowKey");
        }
    }
}