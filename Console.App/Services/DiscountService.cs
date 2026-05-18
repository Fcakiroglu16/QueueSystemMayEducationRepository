using Console.App.ObserverDesignPattern;

namespace Console.App.Services
{
    internal class DiscountService : IDiscountService, IUserObserver
    {
        public void Discount()
        {
            throw new NotImplementedException();
        }

        public void Send(UserCreatedEvent userCreatedEvent)
        {
            System.Console.WriteLine(
                $"Discount :User Created: {userCreatedEvent.UserName}, Email: {userCreatedEvent.Email}");
        }
    }
}
