using System.Collections.Generic;

namespace Azure.EntityServices.Tables.Core
{

    public interface IEntityDataReader<out T>
    {
        IDictionary<string, object> ReadMetadata();

        T Read();
    }
}