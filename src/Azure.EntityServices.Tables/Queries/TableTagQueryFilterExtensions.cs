using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables.Core;
using System.Linq;

namespace Azure.EntityServices.Tables
{
    /// <summary>
    /// Extend table query capabilities to use it for tag filters
    /// </summary>
    public static class TableTagQueryFilterExtensions
    { 

        public static IFilterOperator<T> Between<T, P>(this ITagQueryFilter<T, P> query, P minValue, P maxValue)
        {
            return (query as IQueryFilter<T>)
                  .GreaterThan($"{TableQueryHelper.ToTagRowKeyPrefix(query.TagName, minValue)}")
                  .AndRowKey()
                  .LessThan($"{TableQueryHelper.ToTagRowKeyPrefix(query.TagName, maxValue)}~");
        }

        public static IFilterOperator<T> Equal<T, P>(this ITagQueryFilter<T, P> query, P value)
        {
            return (query as IQueryFilter<T>)
                  .GreaterThan($"{TableQueryHelper.ToTagRowKeyPrefix(query.TagName, value)}")
                  .AndRowKey()
                  .LessThan($"{TableQueryHelper.ToTagRowKeyPrefix(query.TagName, value)}~");
        }

        public static IFilterOperator<T> GreaterThan<T, P>(this ITagQueryFilter<T, P> query, P value)
        {
            return (query as IQueryFilter<T>)
               .GreaterThan($"{TableQueryHelper.ToTagRowKeyPrefix(query.TagName, value)}~")
               .AndRowKey()
               .LessThan($"~{query.TagName}-~");
        }

        public static IFilterOperator<T> GreaterThanOrEqual<T, P>(this ITagQueryFilter<T> query, P value)
        {
            return (query as IQueryFilter<T>)
               .GreaterThan($"{TableQueryHelper.ToTagRowKeyPrefix(query.TagName, value)}")
               .AndRowKey()
               .LessThan($"~{query.TagName}-~");
        }

        public static IFilterOperator<T> LessThan<T, P>(this ITagQueryFilter<T> query, P value)
        {
            return (query as IQueryFilter<T>)
               .GreaterThan($"~{query.TagName}-")
               .AndRowKey()
               .LessThan($"{TableQueryHelper.ToTagRowKeyPrefix(query.TagName, value)}");
        }

        public static IFilterOperator<T> LessThanOrEqual<T, P>(this ITagQueryFilter<T> query, P value)
        {
            return (query as IQueryFilter<T>)
               .GreaterThan($"~{query.TagName}-")
               .AndRowKey()
               .LessThan($"{TableQueryHelper.ToTagRowKeyPrefix(query.TagName, value)}~");
        } 
    }
}