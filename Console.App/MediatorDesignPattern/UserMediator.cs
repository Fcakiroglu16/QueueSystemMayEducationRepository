namespace Console.App.MediatorDesignPattern
{
    internal class UserMediator
    {
        private readonly List<IUserCommandHandler> _handlers = new();

        public void Register(IUserCommandHandler handler)
        {
            _handlers.Add(handler);
        }

        public void Unregister(IUserCommandHandler handler)
        {
            _handlers.Remove(handler);
        }

        public void Publish(CreateUserCommand command)
        {
            foreach (var handler in _handlers)
            {
                handler.Handle(command);
            }
        }
    }
}
