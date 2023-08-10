﻿using Azure.EntityServices.Tables.Core;
using System.Collections.Generic;

namespace Azure.EntityServices.Tables
{
    public interface IEntityObserver<T> : IAsyncObserver<IEnumerable<EntityContext<T>>>
    {
        string Name { get; }
    }
}