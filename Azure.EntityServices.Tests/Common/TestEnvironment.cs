using System;

namespace Azure.EntityServices.Tests.Common
{
    public static class TestEnvironment
    {
        public static string ConnectionString => Environment.GetEnvironmentVariable("ConnectionString") ?? "UseDevelopmentStorage=true" ;
    }
}