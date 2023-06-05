namespace Azure.EntityServices.Blobs
{
    public class EntityBlobClientOptions
    {
        public EntityBlobClientOptions() { }
        public EntityBlobClientOptions(string containerName, int maxResultPerPage = 100)
        { 
            ContainerName = containerName;
            MaxResultPerPage = maxResultPerPage;
        }
        public string ContainerName { get; set; }
        public int? MaxResultPerPage { get; set; }
    }
}