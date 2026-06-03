using Confluent.Kafka;

namespace Kafka.Rest.Producer
{
    public class ValueSerializer<T> : ISerializer<T>
    {
        public byte[] Serialize(T data, SerializationContext context)
        {
            return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(data);
        }
    }
}
