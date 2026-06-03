namespace Shared.Kafka
{
    public record UserCreatedEvent(int Id, string UserName, string Email);
}
