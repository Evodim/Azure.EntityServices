using System;

namespace Common.Samples
{
    public static class TestEnvironment
    {
        public static string ConnectionString =>
            Environment.GetEnvironmentVariable("TEST_STORAGE_CONNECTION_STRING") ?? "UseDevelopmentStorage=true" ;
    }
}