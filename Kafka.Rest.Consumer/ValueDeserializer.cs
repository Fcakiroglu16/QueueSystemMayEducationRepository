using System.Text.Json;
using Confluent.Kafka;

namespace Kafka.Rest.Consumer
{
    public class ValueDeserializer<T> : IDeserializer<T>
    {
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            return JsonSerializer.Deserialize<T>(data)!;
        }
    }
}
