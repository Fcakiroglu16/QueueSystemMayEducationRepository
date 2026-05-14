using System;
using System.Collections.Generic;
using System.Text;
using Console.App.ObserverDesignPattern;

namespace Console.App.Services
{
    internal class EmailService : IEmailService, IUserObserver
    {
        public void send()
        {
            throw new NotImplementedException();
        }

        public void Send(UserCratedEvent userCratedEvent)
        {
            System.Console.WriteLine(
                $"Email :User Created: {userCratedEvent.UserName}, Email: {userCratedEvent.Email}");
        }
    }
}
