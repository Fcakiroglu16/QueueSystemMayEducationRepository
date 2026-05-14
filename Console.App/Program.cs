using Console.App;
using Console.App.ObserverDesignPattern;
using Console.App.Services;

System.Console.Write("Hello, World!");

var userSubject = new UserSubject();

userSubject.Attach(new EmailService());
userSubject.Attach(new DiscountService());


var userService = new UserService(new UserRepository(), userSubject);

userService.CreateUser(new CreateUser("ahmet16", "ahmet16@example.com"));
