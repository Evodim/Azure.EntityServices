using Azure.EntityServices.Tables.Extensions;
using System;
using System.Linq.Expressions;

namespace Azure.EntityServices.Tables
{
    /// <summary>
    /// Helpers to extend filter expression with partition key et row key filters abstraction
    /// </summary>
    public static class ITagQueryComposeExtensions
    {
        public static ITagQueryFilter<T> WhereTag<T>(this ITagQuery<T> query) =>

            (query as ITagQueryCompose<T>).AddTagQuery("RowKey");

        public static ITagQueryFilter<T> WhereTag<T>(this ITagQuery<T> query, string tagName)
        {
            (query as ITagQueryCompose<T>).TagName = tagName;
            return (query as ITagQueryCompose<T>).AddTagQuery("RowKey");
        }

        public static ITagQueryFilter<T> WhereTag<T, P>(this ITagQuery<T> query, Expression<Func<T, P>> tagSelector)
        {
            (query as ITagQueryCompose<T>).TagName = tagSelector.GetPropertyInfo().Name;
            return (query as ITagQueryCompose<T>).AddTagQuery("RowKey");
        }
    }
}