using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public interface IServiceBagBuilder<out K, in T>
    {
        void SetupFactory(Func<K, IServiceProvider, T> factory);
    }
}