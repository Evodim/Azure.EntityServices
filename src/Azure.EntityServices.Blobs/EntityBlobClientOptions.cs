namespace Azure.EntityServices.Blobs
{
    public class EntityBlobClientOptions
    {
        public EntityBlobClientOptions() { }
        public EntityBlobClientOptions(string connectionString, string container)
        {
            ConnectionString = connectionString;
            Container = container;
        }
        public string Container { get; set; }
        public string ConnectionString { get; set; }
    }
}