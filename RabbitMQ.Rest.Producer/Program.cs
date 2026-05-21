using RabbitMQ.Client;
using RabbitMQ.Rest.Producer;
using Scalar.AspNetCore;
using ServiceBus.Shared;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();


builder.Services.AddSingleton<IConnection>(sp =>
{
    var factory = new ConnectionFactory()
    {
        Uri = new Uri(builder.Configuration.GetConnectionString("RabbitMq")!)
    };
    return factory.CreateConnectionAsync().Result;
});

builder.Services.AddSingleton<RabbitMqService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
    app.MapScalarApiReference();
}

app.UseHttpsRedirection();


app.MapGet("send-message-with-no-ack", async (RabbitMqService rabbitMqService) =>
{
    await rabbitMqService.PublishAsDirectExchange(new UserCreatedEvent(1, "ahmet16", "ahmet16@outlook.com"), Ack.Yes);
    return Results.Ok("Message sent");
});

app.MapGet("send-message-with-ack", async (RabbitMqService rabbitMqService) =>
{
    await rabbitMqService.Publish(new UserCreatedEvent(2, "mehmet16", "mehmet16@outlook.com"), Ack.Yes);
    return Results.Ok("Message sent");
});


app.Run();

