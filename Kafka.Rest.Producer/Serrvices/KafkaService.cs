using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Shared.Kafka;

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
                        ReplicationFactor = 1, // leader partition only, no replicas
                        Configs = new Dictionary<string, string>
                        {
                            { "retention.ms", "-1" }
                        }
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

        public async Task SendAtMostOnceComplexMessage()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9094"
            };

            using var producer = new ProducerBuilder<int, UserCreatedEvent>(config)
                .SetValueSerializer(new ValueSerializer<UserCreatedEvent>()).Build();

            try
            {
                var userCreatedEvent = new UserCreatedEvent(10, "ahmet", "ahmet@outlook.com");
                var message =
                    new Message<int, UserCreatedEvent>
                    {
                        Value = userCreatedEvent,
                        Headers = new Headers
                        {
                            { "idempotency-key", Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()) }
                        },
                        Key = userCreatedEvent.Id
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

        public async Task SendAtLeastOnceComplexMessage()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9094",
                Acks = Acks.All,
                MessageSendMaxRetries = 3,
                RetryBackoffMs = 1000
            };

            using var producer = new ProducerBuilder<int, UserCreatedEvent>(config)
                .SetValueSerializer(new ValueSerializer<UserCreatedEvent>()).Build();

            try
            {
                var userCreatedEvent = new UserCreatedEvent(10, "ahmet", "ahmet@outlook.com");
                var message =
                    new Message<int, UserCreatedEvent>
                    {
                        Value = userCreatedEvent,
                        Headers = new Headers
                        {
                            { "idempotency-key", Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()) }
                        },
                        Key = userCreatedEvent.Id
                    };
                var result =
                    await producer.ProduceAsync(TopicName, message);
                Console.WriteLine($"Message sent to partition {result.Partition} with offset {result.Offset}");
            }
            catch (ProduceException<int, UserCreatedEvent> ex)
            {
                Console.WriteLine($"An error occurred producing message: {ex.Error.Reason}");
            }
        }


        public async Task SendExactlyOnceComplexMessage()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9094",
                Acks = Acks.All,
                MessageSendMaxRetries = 3,
                RetryBackoffMs = 1000,
                EnableIdempotence = true
            };

            using var producer = new ProducerBuilder<int, UserCreatedEvent>(config)
                .SetValueSerializer(new ValueSerializer<UserCreatedEvent>()).Build();

            try
            {
                var userCreatedEvent = new UserCreatedEvent(10, "ahmet", "ahmet@outlook.com");
                var message =
                    new Message<int, UserCreatedEvent>
                    {
                        Value = userCreatedEvent,
                        Headers = new Headers
                        {
                            { "idempotency-key", Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()) }
                        },
                        Key = userCreatedEvent.Id
                    };
                var result =
                    await producer.ProduceAsync(TopicName, message);
                Console.WriteLine($"Message sent to partition {result.Partition} with offset {result.Offset}");
            }
            catch (ProduceException<int, UserCreatedEvent> ex)
            {
                Console.WriteLine($"An error occurred producing message: {ex.Error.Reason}");
            }
        }


        public async Task SendComplexMessageWithTransaction()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9094",
                Acks = Acks.All,
                MessageSendMaxRetries = 3,
                RetryBackoffMs = 1000,
                TransactionalId = "transactional-id-1",
                EnableIdempotence = true
            };

            using var producer = new ProducerBuilder<int, UserCreatedEvent>(config)
                .SetValueSerializer(new ValueSerializer<UserCreatedEvent>()).Build();

            try
            {
                var userCreatedEvent = new UserCreatedEvent(10, "ahmet", "ahmet@outlook.com");
                var message =
                    new Message<int, UserCreatedEvent>
                    {
                        Value = userCreatedEvent,
                        Headers = new Headers
                        {
                            { "idempotency-key", Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()) }
                        },
                        Key = userCreatedEvent.Id
                    };

                producer.InitTransactions(TimeSpan.FromSeconds(10));

                producer.BeginTransaction();

                try
                {
                    var result =
                        await producer.ProduceAsync(TopicName, message);

                    Console.WriteLine($"Message sent to partition {result.Partition} with offset {result.Offset}");
                    var result2 =
                        await producer.ProduceAsync(TopicName, message);


                    producer.CommitTransaction();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);

                    producer.AbortTransaction();
                }
            }
            catch (ProduceException<int, UserCreatedEvent> ex)
            {
                Console.WriteLine($"An error occurred producing message: {ex.Error.Reason}");
            }
        }
    }
}
