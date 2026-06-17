using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ServiceBus.Shared;

namespace RabbitMQ.Rest.Consumer.BackgroundServices
{
    public class UserCreatedEventStreamConsumer(IConnection connection, ILogger<UserCreatedEventConsumer> logger)
        : BackgroundService
    {
        //hooks
        //foreground thread //background thread

        private IChannel channel;

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
            await base.StartAsync(cancellationToken);
        }

        //graceful shutdown
        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            await channel.DisposeAsync();

            await base.StopAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var consumer = new AsyncEventingBasicConsumer(channel);

            consumer.ReceivedAsync += async (sender, args) =>
            {
                long queueMessageOffset = 0;
                if (args.BasicProperties.Headers?.TryGetValue("x-stream-offset", out var offsetObj) == true &&
                    offsetObj is long offset)
                {
                    queueMessageOffset = offset;
                }


                var body = args.Body.ToArray();
                var message = System.Text.Encoding.UTF8.GetString(body);

                var userCreatedEvent = JsonSerializer.Deserialize<UserCreatedEvent>(message);

                logger.LogInformation(
                    $"Received message(RabbitMq.Rest.Consumer): Id : {userCreatedEvent?.Id}, Name: {userCreatedEvent?.UserName}, Email: {userCreatedEvent?.Email}, offset: {queueMessageOffset.ToString()}");

                await channel.BasicAckAsync(args.DeliveryTag, true, stoppingToken);


                await Task.CompletedTask;
            };

            var consumerArgs = new Dictionary<string, object>
            {
                { "x-stream-offset", "first" }
            };


            await channel.BasicConsumeAsync("stream-queue", false, "rest-consumer-tag", arguments: consumerArgs,
                consumer, stoppingToken);
        }
    }
}
