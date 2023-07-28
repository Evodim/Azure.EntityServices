using Azure.EntityServices.Queries;
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
        /// <summary>
        /// Only retrieve non tagged entities, tagged entities partitionKey and rowKey does not start with reserved '~' character
        /// Replicated rows are isolated by this keyword
        /// By default, tagged entities was already ignored 
        /// </summary>
        public static IFilterOperator<T> IgnoreTags<T>(this IQuery<T> query)

         => (query as IQueryCompose<T>)
               .AddQuery("PartitionKey")
               .LessThan("~")
               .AndRowKey()
               .LessThan("~");

        /// <summary>
        /// Include tagged entities (replicated partitions and rows)
        /// By default, tagged entities was ignored 
        /// </summary>
        public static IQuery<T> IncludeTags<T>(this IQuery<T> query)
        {
            (query as ITagQueryCompose<T>).TagName = " ";

            return query;
        }

        /// <summary>
        /// Get only tagged partitions and rows that start with "~" character 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public static IFilterOperator<T> WithOnlyTags<T>(this IQuery<T> query)

        {
            (query as ITagQueryCompose<T>).TagName = " ";
            return (query as IQueryCompose<T>)
                .AddQuery("PartitionKey")
                .GreaterThan("~")
                .AndRowKey()
                .GreaterThan("~");
        }

        /// <summary>
        /// Allow to retrieve all entities for a specific tag (tagged entities)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="tagName"></param>
        /// <returns></returns>
        public static IFilterOperator<T> WithTag<T>(this IQuery<T> query, string tagName)
        {
            (query as ITagQueryCompose<T>).TagName = " ";
            return query
              .WhereRowKey()
              .GreaterThan($"~{tagName}-")
              .AndRowKey()
              .LessThan($"~{tagName}-~");
        }

        /// <summary>
        /// Allow to retrieve all entities for a given tag as property selector
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="P"></typeparam>
        /// <param name="query"></param>
        /// <param name="tagSelector"></param>
        /// <returns></returns>
        public static IFilterOperator<T> WithTag<T, P>(this IQuery<T> query, Expression<Func<T, P>> tagSelector)
        {
            var tagName = tagSelector.GetPropertyInfo().Name;
            (query as ITagQueryCompose<T>).TagName = " ";
            return query
              .WhereRowKey()
              .GreaterThan($"~{tagName}-")
              .AndRowKey()
              .LessThan($"~{tagName}-~");
        }

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