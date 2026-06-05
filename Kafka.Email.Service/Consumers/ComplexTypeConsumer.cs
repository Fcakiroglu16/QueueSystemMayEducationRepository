using Confluent.Kafka;
using Shared.Kafka;

namespace Kafka.Email.Service.Consumers
{
    public class ComplexTypeConsumer(ILogger<ComplexTypeConsumer> logger) : BackgroundService
    {
        private ConsumerConfig consumerConfig;

        //hooks
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            consumerConfig = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "Kafka.Rest.Consumer",
                AutoOffsetReset = AutoOffsetReset.Earliest,
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


                    var userCreatedEvent = result.Message.Value;

                    // add log message

                    logger.LogInformation("Consumed message: {UserCreatedEvent}", userCreatedEvent.UserName);
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
