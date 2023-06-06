namespace Azure.EntityServices.Tables.Core
{
    internal static class EntityAdapterExtensions
    {
        public static IEntityAdapter<T> CopyMetadataTo<T>(this IEntityAdapter<T> source, IEntityAdapter<T> target)
            where T : class, new()
        {
            foreach (var metadata in source.Metadata)
            {
                target.Metadata.Add(metadata);
            }
            return source;
        }
    }
}