using Console.App.MediatorDesignPattern;

namespace Console.App.Handlers
{
    internal class DiscountHandler : IUserCommandHandler
    {

        public void Handle(CreateUserCommand command)
        {
            System.Console.WriteLine(
                $"Discount :User Created: {command.UserName}, Email: {command.Email}");
        }
    }
}
