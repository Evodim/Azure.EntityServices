namespace Azure.EntityServices.Blobs
{
    public class BlobServiceOptions
    {
        public string ContainerName { get; set; }
        public int MaxResultPerPage { get; set; } = 100;
    }
}