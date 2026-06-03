using Confluent.Kafka;
using Shared.Kafka;

namespace Kafka.Rest.Consumer.Consumers
{
    public class ComplexTypeConsumerWithAck(ILogger<ComplexTypeConsumerWithAck> logger) : BackgroundService
    {
        private ConsumerConfig consumerConfig;

        //hooks
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            consumerConfig = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "kafka-rest-consumer-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false // disable auto-commit for manual acknowledgment
            };


            return base.StartAsync(cancellationToken);
        }

        //graceful shutdown
        public override Task StopAsync(CancellationToken cancellationToken)
        {
            return base.StopAsync(cancellationToken);
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var consumer = new ConsumerBuilder<int, UserCreatedEvent>(consumerConfig)
                .SetValueDeserializer(new ValueDeserializer<UserCreatedEvent>()).Build();
            consumer.Subscribe("user.created.event-topic");
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(5));
                try
                {
                    if (result is null)
                    {
                        continue;
                    }

                    var userCreatedEvent = result.Message.Value;

                    // add log message

                    logger.LogInformation("Consumed message: {UserCreatedEvent}", userCreatedEvent.UserName);


                    consumer.Commit(result);
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
