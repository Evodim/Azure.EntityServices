using System.Text.Json;
using System.Text.Json.Serialization;

namespace Azure.EntityServices.Tables.Extensions
{
    public static class JSonSerializerOptionsExtensions
    {
        public static JsonSerializerOptions UseNamingPolicy(this JsonSerializerOptions serializerOptions, JsonNamingPolicy jsonNamingPolicy)
        {
            serializerOptions.PropertyNamingPolicy = jsonNamingPolicy;

            return serializerOptions;
        }
        public static JsonSerializerOptions UseCamelCaseNamingPolicy(this JsonSerializerOptions serializerOptions)
        {
            serializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;

            return serializerOptions;
        }
        public static JsonSerializerOptions AddConverter(this JsonSerializerOptions serializerOptions, JsonConverter jsonConverter)
        {
            serializerOptions.Converters.Add(jsonConverter);

            return serializerOptions;
        }
        public static JsonSerializerOptions AddJsonStringEnumConverter(this JsonSerializerOptions serializerOptions)
        {
            serializerOptions.Converters.Add(new JsonStringEnumConverter());
            return serializerOptions;
        }
    }
}