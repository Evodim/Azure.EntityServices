namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public static class EntityTableClientBuilderExtensions
    {
        public static IEntityTableClientBuilder<T> RegisterObserver<T, TImplementation>(this IEntityTableClientBuilder<T> builder, string observerName)
            where TImplementation : IEntityObserver<T>
        {
            return ((EntityTableClientBuilder<T>)builder)
                .RegisterObserver<TImplementation>(observerName);
        }
    }
}