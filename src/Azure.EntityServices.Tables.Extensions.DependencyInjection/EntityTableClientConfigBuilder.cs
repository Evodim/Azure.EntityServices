using Microsoft.Extensions.DependencyInjection;
using System;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{

    public class EntityTableClientBuilder<T>: IEntityTableClientBuilder<T>
    {
        public  EntityTableClientOptions Options { get; } = new EntityTableClientOptions();
        public EntityTableClientConfig<T> Config { get; } = new EntityTableClientConfig<T>();
        
        public EntityTableClientBuilder(IServiceCollection serviceCollection)
        { 
            ServiceProvider = null;
            ServiceCollection = serviceCollection;
            EntityObserverBuilder = serviceCollection.AddByName<IEntityObserver<T>>();
        }

        public Action<EntityTableClientOptions> OptionsAction { get; private set; }
        public Action<EntityTableClientConfig<T>> ConfigAction { get; private set; }
        public IServiceProvider ServiceProvider { get; private set; }
        public IServiceCollection ServiceCollection { get; }
        public ServicesByNameBuilder<IEntityObserver<T>> EntityObserverBuilder { get; }

        public EntityTableClientOptions BindOptions()
        { 
            OptionsAction.Invoke(Options); 
            return Options;
        }
        public EntityTableClientConfig<T> BindConfig(IServiceProvider provider)
        {
            ServiceProvider = provider;
            ConfigAction.Invoke(Config);
            return Config;
        }

        public (EntityTableClientOptions,EntityTableClientConfig<T>) Build()
        {
          
            EntityObserverBuilder?.Build();  
            return (Options, Config);
        }

        public IEntityTableClientBuilder<T> ConfigureEntity(Action<EntityTableClientConfig<T>> configurator)
        {
            ConfigAction = configurator;
            return this;
        }

        public IEntityTableClientBuilder<T> ConfigureOptions(Action<EntityTableClientOptions> optionsConfigurator)
        {
            OptionsAction = optionsConfigurator;
            return this;
        }
    }
}