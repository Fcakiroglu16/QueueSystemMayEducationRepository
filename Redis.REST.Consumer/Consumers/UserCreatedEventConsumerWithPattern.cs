using System.Text.Json;
using Redis.Shared;
using StackExchange.Redis;

namespace Redis.REST.Consumer.Consumers
{
    public class UserCreatedEventConsumerWithPattern(RedisService redisService) : BackgroundService
    {
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var subscriber = redisService.Subscriber;

            subscriber.Subscribe(new RedisChannel("info.*.*", RedisChannel.PatternMode.Pattern), (channel, message) =>
            {
                var userCreatedEvent = JsonSerializer.Deserialize<UserCreatedEvent>(message.ToString());
                Console.WriteLine($"User created: {userCreatedEvent!.Name}, {userCreatedEvent?.Email}");
            });

            return Task.CompletedTask;
        }
    }
}
