using Azure.EntityServices.Tables.Core;

namespace Azure.EntityServices.Tables
{
    public class EntityContext<T>  : IEntityContext<T>
    where T : class, new()
    {
        public EntityContext(IEntityAdapter<T> entityBinder, EntityOperation entityOperation)
        {
            EntityAdapter = entityBinder;
            EntityOperation = entityOperation;
        }
        public IEntityAdapter<T> EntityAdapter { get; }

        public EntityOperation EntityOperation { get; }
    }
}