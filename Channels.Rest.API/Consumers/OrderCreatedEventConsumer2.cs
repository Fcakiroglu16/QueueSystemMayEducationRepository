using System.Threading.Channels;
using Channels.Rest.API.Events;

namespace Channels.Rest.API.Consumers
{
    public class OrderCreatedEventConsumer2(Channel<OrderCreatedEvent> channel) : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await foreach (var orderCreatedEvent in channel.Reader.ReadAllAsync(stoppingToken))
            {
                // Process the user created event
                Console.WriteLine($"Order created 2: {orderCreatedEvent.Id}, {orderCreatedEvent.ProductName}");
            }
        }
    }
}
