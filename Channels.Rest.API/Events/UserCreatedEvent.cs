namespace Channels.Rest.API.Events
{
    public record UserCreatedEvent(int Id, string Name, string Email);
}
