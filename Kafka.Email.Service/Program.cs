using Kafka.Email.Service;
using Kafka.Email.Service.Consumers;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<ComplexTypeConsumer>();

var host = builder.Build();
host.Run();
