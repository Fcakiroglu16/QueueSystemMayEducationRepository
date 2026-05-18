namespace Console.App.ObserverDesignPattern
{
    internal class UserSubject
    {
        private readonly List<IUserObserver> _observers = new();

        public void Notify(UserCreatedEvent userCreatedEvent)
        {
            foreach (var observer in _observers)
            {
                observer.Send(userCreatedEvent);
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
