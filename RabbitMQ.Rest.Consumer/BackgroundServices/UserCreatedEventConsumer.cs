using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ServiceBus.Shared;

namespace RabbitMQ.Rest.Consumer.BackgroundServices
{
    public class UserCreatedEventConsumer(IConnection connection, ILogger<UserCreatedEventConsumer> logger)
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
            await channel.QueueDeclareAsync("rabbitmq-rest-consumer.user-created.event", true, false, false, null,
                cancellationToken: stoppingToken);


            await channel.QueueBindAsync("rabbitmq-rest-consumer.user-created.event",
                "rabbitmq-rest-producer.user-created.exchange", string.Empty, cancellationToken: stoppingToken);


            var consumer = new AsyncEventingBasicConsumer(channel);


            consumer.ReceivedAsync += async (sender, args) =>
            {
                var body = args.Body.ToArray();
                var message = System.Text.Encoding.UTF8.GetString(body);

                var userCreatedEvent = JsonSerializer.Deserialize<UserCreatedEvent>(message);

                logger.LogInformation(
                    $"Received message(RabbitMq.Rest.Consumer): Id : {userCreatedEvent?.Id}, Name: {userCreatedEvent?.UserName}, Email: {userCreatedEvent?.Email}");


                await channel.BasicAckAsync(args.DeliveryTag, false);

                await Task.CompletedTask;
            };


            await channel.BasicConsumeAsync("rabbitmq-rest-consumer.user-created.event", false, consumer,
                cancellationToken: stoppingToken);
        }
    }
}
