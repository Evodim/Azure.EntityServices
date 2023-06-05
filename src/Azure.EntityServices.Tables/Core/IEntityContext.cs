using Azure.EntityServices.Tables.Core;

namespace Azure.EntityServices.Tables
{
    public interface IEntityContext<T>
    {
        IEntityAdapter<T> EntityAdapter { get; }
        EntityOperation EntityOperation { get; }
    }
}