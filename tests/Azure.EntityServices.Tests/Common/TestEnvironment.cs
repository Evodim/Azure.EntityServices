using System;

namespace Azure.EntityServices.Tests.Common
{
    public static class TestEnvironment
    {
        public static string ConnectionString =>
            Environment.GetEnvironmentVariable("TEST_STORAGE_CONNECTION_STRING") ?? "UseDevelopmentStorage=true" ;
    }
}