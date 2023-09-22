using System.Collections.Generic;

namespace BlobClient.BasicSample
{
    public class CountryRoadsEntity
    {
        public string CountryCode { get; set; }
        public List<RoadItem> Roads { get; set; }
    }

    public readonly record struct Properties(double[] geo_point_2d, string icc);

    public readonly record struct Geometry(double[][] coordinates, string type);

    public readonly record struct RoadItem(string type, Geometry geometry, Properties properties);
}