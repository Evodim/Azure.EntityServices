namespace Azure.EntityServices.Tables.Extensions
{
    /// <summary>
    /// Helpers to extend filter expression with partition key et row key filters abstraction
    /// </summary>
    public static class ITagQueryComposeExtensions
    {
        public static IQueryTagFilter<T> WhereTag<T>(this ITagQueryCompose<T> query) => query.AddTagQuery("RowKey");

        public static IQueryTagFilter<T> AndTag<T>(this ITagQueryCompose<T> query) => query.AddTagQuery("RowKey");
    }
}