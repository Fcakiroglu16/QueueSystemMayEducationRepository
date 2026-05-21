using System.Security.Authentication.ExtendedProtection;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ServiceBus.Shared;

namespace RabbitMQ.Rest.Consumer.BackgroundServices
{
    public class UserCreatedEventConsumerDirectExchange(
        IConnection connection,
        ILogger<UserCreatedEventConsumerDirectExchange> logger)
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
            await channel.BasicQosAsync(0, 5, true, stoppingToken);


            await channel.QueueDeclareAsync("rabbitmq-rest-consumer.user-created.direct.event", true, false, false,
                null,
                cancellationToken: stoppingToken);


            await channel.QueueBindAsync("rabbitmq-rest-consumer.user-created.direct.event",
                "rabbitmq-rest-producer.user-created.direct.exchange", "route-key-x", cancellationToken: stoppingToken);


            var consumer = new AsyncEventingBasicConsumer(channel);


            consumer.ReceivedAsync += async (sender, args) =>
            {
                var body = args.Body.ToArray();
                var message = System.Text.Encoding.UTF8.GetString(body);

                var userCreatedEvent = JsonSerializer.Deserialize<UserCreatedEvent>(message);

                logger.LogInformation(
                    $"Received message(RabbitMq.Rest.Consumer): Id : {userCreatedEvent?.Id}, Name: {userCreatedEvent?.UserName}, Email: {userCreatedEvent?.Email}");

                await channel.BasicAckAsync(args.DeliveryTag, true, stoppingToken);
            };


            await channel.BasicConsumeAsync("rabbitmq-rest-consumer.user-created.direct.event", false, consumer,
                cancellationToken: stoppingToken);
        }
    }
}
