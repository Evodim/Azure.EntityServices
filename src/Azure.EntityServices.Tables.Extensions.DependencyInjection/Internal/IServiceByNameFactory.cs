﻿using System.Collections.Generic;

namespace Azure.EntityServices.Tables.Extensions.DependencyInjection
{
    /// <summary>
    /// Provides instances of registered services by name
    /// </summary>
    /// <typeparam name="TService"></typeparam>
    public interface IServiceByNameFactory<out TService>
    {
        /// <summary>
        /// Provides instance of registered service by name or null if the named type isn't registered in <see cref="Microsoft.Extensions.DependencyInjection.IServiceProvider"/>
        /// </summary>
        TService GetByName(string name);

        /// <summary>
        /// Provides instance of registered service by name.  If type isn't registered an InvalidOperationException will be thrown.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        TService GetRequiredByName(string name);

        /// <summary>
        /// Gets names of all the registered instances of <typeparamref name="TService"/>
        /// </summary>
        /// <returns></returns>
        ICollection<string> GetNames();
    }
}