using System;
using System.Collections.Generic;
using System.Text;

namespace Console.App.ObserverDesignPattern
{
    public record UserCratedEvent(int UserId, string UserName, string Email);
}