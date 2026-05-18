using Console.App.MediatorDesignPattern;

namespace Console.App
{
    internal class UserServiceWithMediator(IUserRepository repository, UserMediator userMediator)
    {
        internal void CreateUser(CreateUser createUser)
        {
            repository.CreateUser(createUser);

            userMediator.Publish(new CreateUserCommand(1, createUser.userName, createUser.email));
        }
    }
}
