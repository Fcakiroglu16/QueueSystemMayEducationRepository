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
            var queueArguments = new Dictionary<string, object>()
            {
                { "x-dead-letter-exchange", "rabbitmq-rest-consumer.user-created.exchange.dlx" },
                { "x-message-ttl", 60000 },
                { "x-max-length", 1000 },
                { "x-max-length-bytes", 1_073_741_824 },
                { "x-queue-type", "Classic" },
                //{ "x-delivery-limit", 5 }
            };


            await channel.QueueDeclareAsync("rabbitmq-rest-consumer.user-created.event", true, false, false,
                queueArguments,
                cancellationToken: stoppingToken);


            await channel.QueueBindAsync("rabbitmq-rest-consumer.user-created.event",
                "rabbitmq-rest-producer.user-created.exchange", string.Empty, cancellationToken: stoppingToken);


            //Dead letter exchange

            await channel.ExchangeDeclareAsync("rabbitmq-rest-consumer.user-created.exchange.dlx", ExchangeType.Fanout,
                true, false, null, cancellationToken: stoppingToken);


            await channel.QueueDeclareAsync("rabbitmq-rest-consumer.user-created.event.dlx.queue", true, false, false,
                null,
                cancellationToken: stoppingToken);


            await channel.QueueBindAsync("rabbitmq-rest-consumer.user-created.event.dlx.queue",
                "rabbitmq-rest-consumer.user-created.exchange.dlx", string.Empty, cancellationToken: stoppingToken);


            var consumer = new AsyncEventingBasicConsumer(channel);


            consumer.ReceivedAsync += async (sender, args) =>
            {
                // IdempotencyKey,eventType,IsProcess,CreateDate


                //if (args.BasicProperties.Headers is not null &&
                //    args.BasicProperties.Headers.TryGetValue("version", out var version))
                //{
                //    if (version.ToString() == "v1")
                //    {
                //        await channel.BasicNackAsync(args.DeliveryTag, false, false, stoppingToken);
                //    }

                //    // Handle version if needed  
                //}

                //if (args.BasicProperties.Headers is not null &&
                //    args.BasicProperties.Headers.TryGetValue("IdempotencyKey", out var idempotencyKey))
                //{
                //    // if( dbContext.Idempotency.any(x=>x.IdempotencyKey == idempotencyKey.ToString() && x.IsProcessed==true))
                //    await channel.BasicNackAsync(args.DeliveryTag, false, false, stoppingToken);

                //    await Task.CompletedTask;
                //    return;
                //    // Handle version if needed  
                //}


                try
                {
                    var body = args.Body.ToArray();
                    var message = System.Text.Encoding.UTF8.GetString(body);

                    var userCreatedEvent = JsonSerializer.Deserialize<UserCreatedEvent>(message);

                    logger.LogInformation(
                        $"Received message(RabbitMq.Rest.Consumer): Id : {userCreatedEvent?.Id}, Name: {userCreatedEvent?.UserName}, Email: {userCreatedEvent?.Email}");

                    // dbContext Order/stock save
                    // IdempotencyKey save
                    // transaction commit

                    throw new Exception("db hatası");
                    await channel.BasicAckAsync(args.DeliveryTag, true, stoppingToken);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    await channel.BasicNackAsync(args.DeliveryTag, false, true, stoppingToken);
                }


                await Task.CompletedTask;
            };


            await channel.BasicConsumeAsync("rabbitmq-rest-consumer.user-created.event", false, consumer,
                cancellationToken: stoppingToken);
        }
    }
}
