using Azure.EntityServices.Tables.Core;

namespace Azure.EntityServices.Tables
{
    public interface IEntityBinderContext<T>
    {
        IEntityBinder<T> EntityBinder { get; }
        EntityOperation EntityOperation { get; }
    }
}