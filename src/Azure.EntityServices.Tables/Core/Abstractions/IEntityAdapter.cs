using System.Collections.Generic;

namespace Azure.EntityServices.Tables.Core.Abstractions
{
    public interface IEntityAdapter<T>
    where T : class, new()
    {
        IDictionary<string, object> GetMetadata(IDictionary<string, object> entityModel);

        T FromEntityModel(IDictionary<string, object> entityModel);

        TEntityModel ToEntityModel<TEntityModel>(T entity) where TEntityModel : class, new();

        EntityOperation ToEntityOperationAction(EntityOperationType entityOperation, T entity);
    }
}