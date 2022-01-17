namespace Azure.EntityServices.Table.Core
{
    internal static class EntityBinderExtensions
    {
        public static IEntityBinder<T> CopyMetadataTo<T>(this IEntityBinder<T> binderSource, IEntityBinder<T> binderDestination)
            where T : class, new()
        {
            foreach (var metadata in binderSource.Metadata)
            {
                binderDestination.Metadata.Add(metadata);
            }
            return binderSource;
        }
    }
}