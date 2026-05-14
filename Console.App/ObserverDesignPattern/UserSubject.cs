using System;
using System.Collections.Generic;
using System.Text;

namespace Console.App.ObserverDesignPattern
{
    public interface IUserObserver
    {
        void Send(UserCratedEvent userCratedEvent);
    }


    internal class UserSubject
    {
        private readonly List<IUserObserver> _observers = new();

        public void Notify(UserCratedEvent userCratedEvent)
        {
            foreach (var observer in _observers)
            {
                observer.Send(userCratedEvent);
            }
        }

        public void Attach(IUserObserver observer)
        {
            _observers.Add(observer);
        }

        public void Detach(IUserObserver observer)
        {
            _observers.Remove(observer);
        }
    }
}
