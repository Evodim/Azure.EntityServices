using Azure.EntityServices.Tables;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tests.Table
{
    internal class EntityTableClientCommon
    {
       public static Action<EntityTableClientOptions> DefaultOptions(string tableName) => (EntityTableClientOptions options) =>
            {
                options.CreateTableIfNotExists = true;
                options.TableName = $"{tableName}{Guid.NewGuid():N}";
            };
}
}
