using Azure.Data.Tables;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace Azure.EntityServices.Tables.Core
{ 
    public interface IEntityAdapter<T,TEntityModel> 
    where T : class, new()
    {
       IDictionary<string, object> GetProperties(TEntityModel entityModel);
       T FromEntityModel(TEntityModel entityModel);
       TEntityModel ToEntityModel(T entity);
    }
}