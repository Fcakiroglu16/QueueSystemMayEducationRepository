using Console.App.MediatorDesignPattern;

namespace Console.App.Handlers
{
    internal class EmailHandler : IUserCommandHandler
    {
        public void Handle(CreateUserCommand command)
        {
            System.Console.WriteLine(
                $"Email :User Created: {command.UserName}, Email: {command.Email}");
        }
    }
}
