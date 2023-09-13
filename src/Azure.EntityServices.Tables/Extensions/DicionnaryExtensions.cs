using System.Collections.Generic;

namespace Azure.EntityServices.Tables.Extensions
{
    internal static class DicionnaryExtensions
    {

        public static IDictionary<T, U> AddOrUpdate<T, U>(this IDictionary<T, U> dico, KeyValuePair<T,U> keyValue)
        {
            if (dico.ContainsKey(keyValue.Key))
            {
                dico[keyValue.Key] = keyValue.Value;
            }
            else
            {
                dico.Add(keyValue);
            }
            return dico;
        }
        public static IDictionary<T, U> AddOrUpdate<T, U>(this IDictionary<T, U> dico, T key, U value)
        {
            if (dico.ContainsKey(key))
            {
                dico[key] = value;
            }
            else
            {
                dico.Add(key, value);
            }
            return dico;
        }
    }
}