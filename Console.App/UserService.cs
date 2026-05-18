using Console.App.Services;
using System;
using System.Collections.Generic;
using System.Text;
using Console.App.ObserverDesignPattern;

namespace Console.App
{
    public record CreateUser(string userName, string email);

    internal class UserService(IUserRepository repository, UserSubject userSubject)
    {
        internal void CreateUser(CreateUser createUser)
        {
            repository.createUser(createUser);

            userSubject.Notify(new UserCratedEvent(1, "ahmet16", "ahmet16@example.com"));
            //Observer Design Pattern
            //Mediator Design Pattern (MediatR Library)
        }
    }

    internal interface IUserRepository
    {
        void createUser(CreateUser createUser1);
    }

    public class UserRepository : IUserRepository
    {
        public void createUser(CreateUser createUser1)
        {
            System.Console.WriteLine($"User Created: {createUser1.userName}, Email: {createUser1.email}");
        }
    }
}
