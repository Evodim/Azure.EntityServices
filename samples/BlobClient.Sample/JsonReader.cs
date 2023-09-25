using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using static System.Net.Mime.MediaTypeNames;

namespace BlobClient.BasicSample
{
    public static class JsonArrayReader
    {
        public static long LoadToPipeline<T>(this Stream jsonStream, ITargetBlock<T> output)
           where T : new()
        {
            long readed = 0;
            using (var jsonStreamReader = new Utf8JsonStreamReader(jsonStream, 32 * 1024))
            {
                var items = new List<T>();
                do
                {
                    jsonStreamReader.Read();
                }
                while (jsonStreamReader.TokenType != JsonTokenType.StartArray);

                jsonStreamReader.Read();

                while (jsonStreamReader.TokenType != JsonTokenType.EndArray)
                {
                    // deserialize object
                    readed++;
                    output.Post(jsonStreamReader.Deserialise<T>());

                    // JsonSerializer.Deserialize ends on last token of the object parsed,
                    // move to the first token of next object
                    jsonStreamReader.Read();

                    if (readed % 1000 == 0)
                    {
                        Console.WriteLine(readed);
                    }
                }
            }
            return readed;
        }



        public static void ForEachRecord<T>(this Stream jsonStream, Action<T> record)
            where T : new()
        {
            using (var jsonStreamReader = new Utf8JsonStreamReader(jsonStream, 32 * 1024))
            {
                do
                {
                    jsonStreamReader.Read();
                }
                while (jsonStreamReader.TokenType != JsonTokenType.StartArray);

                jsonStreamReader.Read();

                while (jsonStreamReader.TokenType != JsonTokenType.EndArray)
                {
                    // deserialize object
                    record.Invoke(jsonStreamReader.Deserialise<T>());

                    // JsonSerializer.Deserialize ends on last token of the object parsed,
                    // move to the first token of next object
                    jsonStreamReader.Read();
                }
            }
        }
    }
}
