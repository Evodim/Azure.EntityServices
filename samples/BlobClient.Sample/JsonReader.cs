using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;

namespace BlobClient.BasicSample
{
    public static class JsonArrayReader
    {
        public static long DeserializeItems<T>(this Stream jsonStream, ITargetBlock<T> output)
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

        public static void DeserializeItems<T>(this Stream jsonStream, Action<T> itemAction)
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
                    itemAction.Invoke(jsonStreamReader.Deserialise<T>());

                    // JsonSerializer.Deserialize ends on last token of the object parsed,
                    // move to the first token of next object
                    jsonStreamReader.Read();
                }
            }
        }
         
    }
}