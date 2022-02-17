namespace Azure.EntityServices.Tables
{
    /// <summary>
    /// Helpers to extend filter expression with partition key et row key filters abstraction
    /// </summary>
    public static class ITagQueryComposeExtensions
    {
        public static ITagQueryFilter<T> WhereTag<T>(this ITagQueryCompose<T> query) => query.AddTagQuery("RowKey");
    }
}