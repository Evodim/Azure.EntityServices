using System.Collections.Generic;

namespace Azure.EntityServices.Tables.Core.Abstractions
{

    public interface IEntityDataReader<out T>
    {
        IDictionary<string, object> ReadMetadata();

        T Read();
    }
}