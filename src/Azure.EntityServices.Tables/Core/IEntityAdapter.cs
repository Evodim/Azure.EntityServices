using Azure.Data.Tables;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace Azure.EntityServices.Tables.Core
{  
    public interface IEntityAdapter<T> 
    where T : class, new()
    {
       IDictionary<string, object> GetMetadata(IDictionary<string, object> entityModel);
       T FromEntityModel(IDictionary<string, object> entityModel);
       TEntityModel ToEntityModel<TEntityModel>(T entity) where TEntityModel : class, new();
       EntityOperation ToEntityOperationAction(EntityOperationType entityOperation, T entity);
       TEntityModel ToEntityModel<TEntityModel>(EntityModel entityModel) where TEntityModel : class, new();
    }
}