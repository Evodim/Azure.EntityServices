using System;
using System.Text.Json;

namespace Azure.EntityServices.Tables
{
    public static class EntityTableclientOptionExtensions
    {
        public static EntityTableClientOptions ConfigureSerializer(this EntityTableClientOptions options,Action<JsonSerializerOptions> serializerConfigurator)
        {
            serializerConfigurator?.Invoke(options.SerializerOptions);
            return options;
        }

        public static EntityTableClientOptions ConfigureSerializerWithStringEnumConverter(this EntityTableClientOptions options)
        {
            options?.SerializerOptions?.AddJsonStringEnumConverter();
            return options;
        }
    }
}