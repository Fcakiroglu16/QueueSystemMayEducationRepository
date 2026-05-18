using RabbitMQ.Client;

namespace RabbitMQ.Rest.Producer
{
    public enum Ack
    {
        Yes,
        No
    }

    public class RabbitMqService(IConnection connection, ILogger<RabbitMqService> logger)
    {
        public async Task Publish<T>(T commandOrEvent, Ack ack)
        {
            IChannel channel;
            if (ack == Ack.Yes)
            {
                channel = await connection.CreateChannelAsync(new CreateChannelOptions(true, true));
            }
            else
            {
                channel = await connection.CreateChannelAsync();
            }


            await channel.ExchangeDeclareAsync(exchange: "rabbitmq-rest-producer.user-created.exchange",
                type: ExchangeType.Fanout, durable: true,
                autoDelete: false);


            var body = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(commandOrEvent);

            var properties = new BasicProperties();
            properties.Headers = new Dictionary<string, object?>()
            {
                { "version", "v1" }
            };

            properties.Persistent = true;
            properties.Expiration = TimeSpan.FromSeconds(30).TotalMilliseconds.ToString();


            // Ack=no,mandatory=true=>
            // ack=yes ,mandatory=true => throw exception
            channel.BasicReturnAsync += (sender, args) =>
            {
                logger.LogWarning("Message returned: {ReplyText}", args.ReplyText);
                return Task.CompletedTask;
            };


            if (ack == Ack.Yes) // retry logic => at least once delivery
            {
                int retryCount = 0;
                // retry  timeout & count
                while (retryCount <= 3)
                {
                    try
                    {
                        await channel.BasicPublishAsync("rabbitmq-rest-producer.user-created.exchange", string.Empty,
                            true, properties,
                            body);


                        break;
                    }
                    catch (Exception ex)
                    {
                        retryCount++;
                        logger.LogError(ex, "Error publishing message. Retry count: {RetryCount}", retryCount);
                        if (retryCount > 3)
                        {
                            throw;
                        }

                        //may be jitter logic
                        await Task.Delay((int)Math.Pow(2, retryCount) * 1000);
                    }
                }
            }
            else // no retry logic => at most once delivery
            {
                await channel.BasicPublishAsync("rabbitmq-rest-producer.user-created.exchange", string.Empty, true,
                    properties,
                    body);
            }

            await channel.DisposeAsync();


            //ACK=NO
        }
    }
}