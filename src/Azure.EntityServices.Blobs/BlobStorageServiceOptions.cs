namespace Azure.EntityServices.Blobs
{
    public class BlobStorageServiceOptions
    {
        public string Container { get; set; }
        public string ConnectionString { get; set; }
        public int ResultPerPage { get; set; } = 100;
    }
}