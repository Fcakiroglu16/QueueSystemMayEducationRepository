using System;
using System.Collections.Generic;
using System.Text;
using Console.App.ObserverDesignPattern;

namespace Console.App.Services
{
    internal class DiscountService : IDiscountService, IUserObserver
    {
        public void discount()
        {
            throw new NotImplementedException();
        }

        public void Send(UserCratedEvent userCratedEvent)
        {
            System.Console.WriteLine(
                $"Discount :User Created: {userCratedEvent.UserName}, Email: {userCratedEvent.Email}");
        }
    }
}
