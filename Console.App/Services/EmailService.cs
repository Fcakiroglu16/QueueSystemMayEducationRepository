using Console.App.ObserverDesignPattern;

namespace Console.App.Services
{
    internal class EmailService : IEmailService, IUserObserver
    {
        public void Send()
        {
            throw new NotImplementedException();
        }

        public void Send(UserCreatedEvent userCreatedEvent)
        {
            System.Console.WriteLine(
                $"Email :User Created: {userCreatedEvent.UserName}, Email: {userCreatedEvent.Email}");
        }
    }
}
