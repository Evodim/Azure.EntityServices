using System;

namespace Azure.EntityServices.Tests.Common
{
    public static class TestEnvironment
    {
        public static string ConnectionString =>
            Environment.GetEnvironmentVariable("TEST_STORAGE_CONNECTION_STRING") ?? throw new NotSupportedException("Environment variable TEST_STORAGE_CONNECTION_STRING not found, it must be setted with your test storage connection string,please note that storage emulators are not supported with EntityBlobClient") ;
    }
}