using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Kafka.Rest.Producer.Serrvices
{
    public class KafkaService
    {
        public string TopicName { get; set; }

        public async Task CreateTopicAsync()
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = "localhost:9094"
            }).Build();


            try
            {
                await adminClient.CreateTopicsAsync(new TopicSpecification[]
                {
                    new TopicSpecification
                    {
                        Name = TopicName,
                        NumPartitions = 3,
                        ReplicationFactor = 1 // leader partition only, no replicas
                    }
                });
            }
            catch (CreateTopicsException ex)
            {
                Console.WriteLine($"An error occurred creating topic {TopicName}: {ex.Results[0].Error.Reason}");
            }
        }


        public async Task SendAtMostOnceSimpleMessage()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9094"
            };

            using var producer = new ProducerBuilder<Null, string>(config).Build();

            try
            {
                var message =
                    new Message<Null, string>
                    {
                        Value = "Hello, Kafka!",
                        Headers = new Headers
                        {
                            { "idempotency-key", Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()) }
                        }
                    };
                var result =
                    await producer.ProduceAsync(TopicName, message);
                Console.WriteLine($"Message sent to partition {result.Partition} with offset {result.Offset}");
            }
            catch (ProduceException<Null, string> ex)
            {
                Console.WriteLine($"An error occurred producing message: {ex.Error.Reason}");
            }
        }
    }
}
