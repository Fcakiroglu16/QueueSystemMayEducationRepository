using Confluent.Kafka;

namespace Kafka.Rest.Consumer.Consumers
{
    public class SimpleTypeConsumer(ILogger<SimpleTypeConsumer> logger) : BackgroundService
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
            var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
            consumer.Subscribe("my-topic");
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(stoppingToken);


                    Console.WriteLine(
                        $"Received message: {result.Message.Value} from partition {result.Partition} with offset {result.Offset}");
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
