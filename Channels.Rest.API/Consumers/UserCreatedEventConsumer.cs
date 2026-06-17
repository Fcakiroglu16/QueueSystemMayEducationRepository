using System.Threading.Channels;
using Channels.Rest.API.Events;

namespace Channels.Rest.API.Consumers
{
    public class UserCreatedEventConsumer(Channel<UserCreatedEvent> channel) : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await foreach (var userCreatedEvent in channel.Reader.ReadAllAsync(stoppingToken))
            {
                // Process the user created event
                Console.WriteLine($"User created: {userCreatedEvent.Id}, {userCreatedEvent.Name}");
            }
        }
    }
}
