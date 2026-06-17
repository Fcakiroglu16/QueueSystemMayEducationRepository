using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;
using Channels.Rest.API.Events;

namespace Channels.Rest.API.Consumers
{
    public class OrderCreatedEventConsumer(Channel<OrderCreatedEvent> channel) : BackgroundService
    {
        // Unprocessed messages are persisted here on shutdown and restored on startup.
        private static readonly string PersistencePath =
            Path.Combine(AppContext.BaseDirectory, "order-created-events.txt");

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            // Restore the messages that were left in the channel during the previous shutdown.
            if (File.Exists(PersistencePath))
            {
                var lines = await File.ReadAllLinesAsync(PersistencePath, cancellationToken);

                foreach (var line in lines)
                {
                    if (string.IsNullOrWhiteSpace(line))
                    {
                        continue;
                    }

                    var orderCreatedEvent = JsonSerializer.Deserialize<OrderCreatedEvent>(line);
                    if (orderCreatedEvent is not null)
                    {
                        await channel.Writer.WriteAsync(orderCreatedEvent, cancellationToken);
                        Console.WriteLine($"Order restored: {orderCreatedEvent.Id}, {orderCreatedEvent.ProductName}");
                    }
                }

                File.Delete(PersistencePath);
            }

            await base.StartAsync(cancellationToken);
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            // Graceful shutdown: stop accepting new messages and let the processing loop drain.
            channel.Writer.TryComplete();


            // Persist messages that were still in the channel so they are not lost.
            var pendingEvents = new List<string>();
            while (channel.Reader.TryRead(out var orderCreatedEvent))
            {
                pendingEvents.Add(JsonSerializer.Serialize(orderCreatedEvent));
            }

            if (pendingEvents.Count > 0)
            {
                await File.WriteAllLinesAsync(PersistencePath, pendingEvents, cancellationToken);
                Console.WriteLine($"{pendingEvents.Count} unprocessed order(s) persisted to '{PersistencePath}'.");
            }

            await base.StopAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await foreach (var orderCreatedEvent in channel.Reader.ReadAllAsync(stoppingToken))
            {
                // Process the order created event
                Console.WriteLine($"Order created: {orderCreatedEvent.Id}, {orderCreatedEvent.ProductName}");
            }
        }
    }
}
