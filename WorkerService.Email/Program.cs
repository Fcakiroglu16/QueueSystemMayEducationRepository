using RabbitMQ.Client;
using WorkerService.Email;
using WorkerService.Email.BackgroundServices;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<IConnection>(sp =>
{
    var factory = new ConnectionFactory()
    {
        Uri = new Uri(builder.Configuration.GetConnectionString("RabbitMq")!)
    };
    return factory.CreateConnectionAsync().Result;
});
builder.Services.AddHostedService<UserCreatedEventConsumer>();

var host = builder.Build();
host.Run();
