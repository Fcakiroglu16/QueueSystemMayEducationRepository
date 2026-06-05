using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Shared.Kafka;
using System.Text;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Kafka.Rest.Producer.Events;

namespace Kafka.Rest.Consumer.Consumers
{
    public class ComplexTypeConsumerWithSpecificPartitionRead(ILogger<ComplexTypeConsumer> logger) : BackgroundService
    {
        private ConsumerConfig? _consumerConfig;
        private CachedSchemaRegistryClient? _schemaRegistryClient;

        //hooks
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            _consumerConfig = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "Kafka.Rest.Consumer8",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            //schema
            var schemaRegistryConfig = new SchemaRegistryConfig()
            {
                Url = "http://localhost:8081"
            };


            _schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig);

            return base.StartAsync(cancellationToken);
        }

        //graceful shutdown
        public override Task StopAsync(CancellationToken cancellationToken)
        {
            return base.StopAsync(cancellationToken);
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var consumer = new ConsumerBuilder<string, OrderCreatedEvent>(_consumerConfig)
                .SetValueDeserializer(new AvroDeserializer<OrderCreatedEvent>(_schemaRegistryClient).AsSyncOverAsync())
                .Build();

            // var topicPartition = new TopicPartition("order.created.event-topic", new Partition(2));

            var x = new TopicPartitionOffset(new TopicPartition("order.created.event-topic", new Partition(2)),
                new Offset(1));

            consumer.Assign(x);


            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(TimeSpan.FromSeconds(5));


                    if (result is null) continue;


                    //if (result.Message.Headers.TryGetLastBytes("idempotency-key", out var idempotencyKeyBytes))
                    //{
                    //    var idempotencyKey = Encoding.UTF8.GetString(idempotencyKeyBytes);
                    //    // Use the idempotencyKey as needed
                    //    continue;
                    //}

                    var orderCreatedEvent = result.Message.Value;

                    // add log message

                    logger.LogInformation("Consumed message: {OrderCreatedEvent}", orderCreatedEvent.CustomerId);
                }
                catch (Exception ex)
                {
                    // add log message

                    logger.LogError(ex, "An error occurred while consuming messages.");

                    // Handle cancellation gracefully
                    //
                }
            }

            return Task.CompletedTask;
        }
    }
}
