using System.IO;
using System.Text;

namespace Azure.EntityServices.Blobs.Extensions
{
    public static class StreamExtensions
    {
        public static string ReadAsString(this Stream stream)
        {
            using var reader = new StreamReader(stream.ResetPosition(), Encoding.UTF8);
            return reader.ReadToEnd();
        }

        public static string ReadAsString(this Stream stream, Encoding encoding)
        {
            using var reader = new StreamReader(stream.ResetPosition(), encoding);
            return reader.ReadToEnd();
        }
         
        public static Stream ResetPosition(this Stream stream)
        {
            stream.Seek(0, SeekOrigin.Begin);
            return stream;
        }
    }
}