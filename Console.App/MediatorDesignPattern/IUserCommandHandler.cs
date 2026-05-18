namespace Console.App.MediatorDesignPattern
{
    internal interface IUserCommandHandler
    {
        void Handle(CreateUserCommand command);
    }
}
