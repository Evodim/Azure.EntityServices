using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    public interface IEntityTableClientBuilder<T>
    {
        EntityTableClientOptions Options { get; }
        EntityTableClientConfig<T> Config { get; }

        IEntityTableClientBuilder<T> ConfigureEntity(Action<EntityTableClientConfig<T>> configurator);

        IEntityTableClientBuilder<T> ConfigureOptions(Action<EntityTableClientOptions> optionsConfigurator);

        (EntityTableClientOptions, EntityTableClientConfig<T>) Build();
    }
}