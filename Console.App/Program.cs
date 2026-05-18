using Console.App;
using Console.App.Handlers;
using Console.App.MediatorDesignPattern;
using Console.App.ObserverDesignPattern;
using Console.App.Services;

System.Console.WriteLine("Hello, World!");

// Observer Design Pattern

var userSubject = new UserSubject();

userSubject.Attach(new EmailService());
userSubject.Attach(new DiscountService());

var userService = new UserService(new UserRepository(), userSubject);

userService.CreateUser(new CreateUser("ahmet16", "ahmet16@example.com"));

// Mediator Design Pattern

var userMediator = new UserMediator();

userMediator.Register(new EmailHandler());
userMediator.Register(new DiscountHandler());

var userServiceWithMediator = new UserServiceWithMediator(new UserRepository(), userMediator);

userServiceWithMediator.CreateUser(new CreateUser("mehmet16", "mehmet16@example.com"));
