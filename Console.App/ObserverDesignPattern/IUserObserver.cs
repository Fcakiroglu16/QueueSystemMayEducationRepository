namespace Console.App.ObserverDesignPattern
{
    public interface IUserObserver
    {
        void Send(UserCreatedEvent userCreatedEvent);
    }
}
