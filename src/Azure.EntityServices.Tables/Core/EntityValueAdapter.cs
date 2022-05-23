using Azure.EntityServices.Tables.Extensions;
using System;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.Json;

namespace Azure.EntityServices.Tables.Core
{
    public static class EntityValueAdapter
    {
        /// <summary>
        /// set or convert any given entity prop value to compatbile value type in azure table entity
        /// </summary>
        /// <param name="value"></param>
        /// <param name="entityProp"></param>
        /// <returns></returns>
        public static object WriteValue(object value, PropertyInfo entityProp = null)
        {
            if (value == null)
            {
                return null;
            }
            var propertyType = entityProp?.PropertyType ?? typeof(object);

            if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                propertyType = propertyType.GetGenericArguments().First();
            }

            if (propertyType.IsEnum)
            {
                return value.ToString();
            }

            return value switch
            {
                //ignore handled type by azure data tables sdk
                int _ => value,
                long _ => value,
                double _ => value,
                string _ => value,
                bool _ => value,
                DateTime date =>  (date == default) ? TableConstants.DateTimeStorageDefault : value,
                DateTimeOffset date => (date == default) ? new DateTimeOffset(TableConstants.DateTimeStorageDefault) : value,
                BinaryData => value,
                byte[] _ => value,
                Guid _ => value,
                //otherwise try to serialize in string
                decimal v => v.ToInvariantString(),
                float v => v.ToInvariantString(),
                _ => JsonSerializer.Serialize(value)
            };
        }

        /// <summary>
        /// get or convert any azure table entity value to given entity property
        /// </summary>
        /// <param name="value"></param>
        /// <param name="entityProp"></param>
        /// <returns></returns>
        public static void ReadValue<T>(T entity, PropertyInfo entityProp, object tablePropValue)
        {
            var propertyType = entityProp.PropertyType;

            // Enforce public getter / setter
            if (entityProp.GetSetMethod() == null ||
                !entityProp.GetSetMethod().IsPublic ||
                entityProp.GetGetMethod() == null ||
                !entityProp.GetGetMethod().IsPublic)
            {
                return;
            }

            //Handle nullable types globally
            if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                propertyType = entityProp.PropertyType.GetGenericArguments().First();
            }
            try
            {
                if (tablePropValue == null)
                {
                    entityProp.SetValue(entity, null, null);
                    return;
                }

                if (tablePropValue is byte[] byteValue && propertyType == typeof(BinaryData))
                {
                    entityProp.SetValue(entity, new BinaryData(byteValue), null);
                    return;
                }
            
                //Azure storage service limitation issue - handle default values for datetime 
                if (tablePropValue is DateTimeOffset dateTime && propertyType == typeof(DateTime))
                {

                    //prevent not supported Azure storage service exception, datetime must be in UTC kind
                    entityProp.SetValue(entity, (dateTime == TableConstants.DateTimeStorageDefault) ? default : dateTime.UtcDateTime, null);
                    return;
                }
                //Azure storage service limitation issue - handle default values for datetime 
                if (tablePropValue is DateTimeOffset dateTimeOffset && propertyType == typeof(DateTimeOffset))
                {
                    entityProp.SetValue(entity, (dateTimeOffset == TableConstants.DateTimeStorageDefault)?default: dateTimeOffset, null);
                    return;
                }

               
                //handle some string based interpolation for common types
                if (tablePropValue is string strPropValue)
                {
                    if (propertyType == typeof(string))
                    {
                        entityProp.SetValue(entity, tablePropValue, null);
                        return;
                    }
                    if (propertyType == typeof(DateTime))
                    {
                        if (DateTime.TryParse(strPropValue, CultureInfo.InvariantCulture, DateTimeStyles.None, out var value))
                        {
                            //prevent datetime to be adjusted with localtime
                            entityProp.SetValue(entity, value.ToUniversalTime(), null);
                            return;
                        }

                        return;
                    }
                    if (propertyType == typeof(DateTimeOffset))
                    {

                        if (DateTimeOffset.TryParse(strPropValue, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var value))
                        {
                            entityProp.SetValue(entity, value, null);
                            return;
                        }

                        return;
                    }
                    if (propertyType == typeof(int))
                    {
                        if (int.TryParse(strPropValue, out var value))
                        {
                            entityProp.SetValue(entity, value, null);
                            return;
                        }

                        return;
                    }
                    if (propertyType == typeof(long))
                    {
                        if (long.TryParse(strPropValue, out var value))
                        {
                            entityProp.SetValue(entity, value, null);
                            return;
                        }

                        return;
                    }
                  
                    if (propertyType == typeof(double))
                    {
                        if (double.TryParse(strPropValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                        {
                            entityProp.SetValue(entity, value, null);
                            return;
                        }
                        return;
                    }
                    if (propertyType == typeof(decimal))
                    {
                        if (decimal.TryParse(strPropValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                        {
                            entityProp.SetValue(entity, value, null);
                        }

                        return;
                    }
                    if (propertyType == typeof(float))
                    {
                        if (float.TryParse(strPropValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                        {
                            entityProp.SetValue(entity, value, null);
                        }

                        return;
                    }
                    if (propertyType == typeof(bool))
                    {
                        if (bool.TryParse(strPropValue, out var value))
                        {
                            entityProp.SetValue(entity, value, null);
                        }

                        return;
                    }
                    if (propertyType.IsEnum)
                    {
                        if (Enum.TryParse(propertyType, strPropValue, out var parsedEnum))
                        {
                            entityProp.SetValue(entity, parsedEnum, null);
                        }

                        return;
                    }
                    //binary data will be stored as base64 string to prevent any unexcepted behavior on reading
                    if (propertyType == typeof(BinaryData))
                    {
                        var binaryAsString = new BinaryData(strPropValue);
                        entityProp.SetValue(entity, binaryAsString, null);

                        return;
                    }
                    if (propertyType.IsClass && !propertyType.IsValueType)
                    {
                        //otherwise  it should be a serialized object
                        entityProp.SetValue(entity, JsonSerializer.Deserialize(strPropValue, propertyType), null);
                        return;
                    }
                    entityProp.SetValue(entity, strPropValue, null);
                    return;
                }

                entityProp.SetValue(entity, tablePropValue, null);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Unable to set entity property {entityProp.Name} from table with type {propertyType.Name}", ex);
            }
        }
    }
}