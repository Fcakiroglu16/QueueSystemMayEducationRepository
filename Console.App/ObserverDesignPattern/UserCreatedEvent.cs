namespace Console.App.ObserverDesignPattern
{
    public record UserCreatedEvent(int UserId, string UserName, string Email);
}