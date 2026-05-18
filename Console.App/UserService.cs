using Console.App.ObserverDesignPattern;

namespace Console.App
{
    public record CreateUser(string userName, string email);

    internal class UserService(IUserRepository repository, UserSubject userSubject)
    {
        internal void CreateUser(CreateUser createUser)
        {
            repository.CreateUser(createUser);

            userSubject.Notify(new UserCreatedEvent(1, createUser.userName, createUser.email));
        }
    }

    internal interface IUserRepository
    {
        void CreateUser(CreateUser createUser);
    }

    public class UserRepository : IUserRepository
    {
        public void CreateUser(CreateUser createUser)
        {
            System.Console.WriteLine($"User Created: {createUser.userName}, Email: {createUser.email}");
        }
    }
}
