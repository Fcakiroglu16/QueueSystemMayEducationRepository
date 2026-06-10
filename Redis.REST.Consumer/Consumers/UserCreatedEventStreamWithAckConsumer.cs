using System.Text.Json;
using Redis.Shared;
using StackExchange.Redis;

namespace Redis.REST.Consumer.Consumers
{
    public class UserCreatedEventStreamWithAckConsumer(RedisService redisService) : BackgroundService
    {
        private const string StreamName = "user_created_stream";
        private const string GroupName = "redis.rest.consumer";


        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var db = redisService.GetDatabase;

            // Check if the consumer group already exists; create it only if it does not.
            if (!await db.KeyExistsAsync(StreamName) ||
                (await db.StreamGroupInfoAsync(StreamName)).All(group => group.Name != GroupName))
            {
                await db.StreamCreateConsumerGroupAsync(StreamName, GroupName, StreamPosition.Beginning);
            }


            while (!stoppingToken.IsCancellationRequested)
            {
                var streamEntries =
                    db.StreamReadGroup(StreamName, GroupName, "consumer1", position: ">", count: 10, false);


                foreach (var streamEntry in streamEntries)
                {
                    var userId = streamEntry.Values.GetValue(0);
                    var userName = streamEntry.Values.GetValue(1);
                    var email = streamEntry.Values.GetValue(2);


                    Console.WriteLine($"userId:{userId}, userName:{userName}, email:{email}");

                    await db.StreamAcknowledgeAsync(StreamName, GroupName, streamEntry.Id);
                }

                // Wait for a short period before checking for new entries
                Thread.Sleep(1000);
            }
        }
    }
}
