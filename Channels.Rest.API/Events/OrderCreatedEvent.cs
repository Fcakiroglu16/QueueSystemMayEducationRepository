namespace Channels.Rest.API.Events
{
    public record OrderCreatedEvent(int Id, string ProductName, int Quantity);
}
