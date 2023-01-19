using Azure.EntityServices.Tables.Core;

namespace Azure.EntityServices.Tables
{
    public class EntityBinderContext<T>  : IEntityBinderContext<T>
    where T : class, new()
    {
        public EntityBinderContext(IEntityBinder<T> entityBinder, EntityOperation entityOperation)
        {
            EntityBinder = entityBinder;
            EntityOperation = entityOperation;
        }
        public IEntityBinder<T> EntityBinder { get; }

        public EntityOperation EntityOperation { get; }
    }
}