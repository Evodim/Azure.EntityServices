using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public interface IEntityTableClientBuilder<T>
    {
        IEntityTableClientBuilder<T> ConfigureEntity(Action<EntityTableClientConfig<T>> configurator);

        IEntityTableClientBuilder<T> ConfigureOptions(Action<EntityTableClientOptions> optionsConfigurator);

        void Build();
    }
}