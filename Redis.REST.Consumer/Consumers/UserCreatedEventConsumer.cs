using System.Text.Json;
using Redis.Shared;

namespace Redis.REST.Consumer.Consumers
{
    public class UserCreatedEventConsumer(RedisService redisService) : BackgroundService
    {
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var subscriber = redisService.Subscriber;

            subscriber.Subscribe("user_created_event_channel", (channel, message) =>
            {
                var userCreatedEvent = JsonSerializer.Deserialize<UserCreatedEvent>(message.ToString());
                Console.WriteLine($"User created: {userCreatedEvent!.Name}, {userCreatedEvent?.Email}");
            });

            return Task.CompletedTask;
        }
    }
}
