using System.Threading.Channels;
using Channels.Rest.API.Consumers;
using Channels.Rest.API.Events;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();


builder.Services.AddSingleton(Channel.CreateUnbounded<UserCreatedEvent>(new UnboundedChannelOptions()
{
}));
builder.Services.AddSingleton(Channel.CreateBounded<OrderCreatedEvent>(new BoundedChannelOptions(100)
{
    SingleReader = true,
    SingleWriter = true,
    FullMode = BoundedChannelFullMode.Wait,
}));


builder.Services.AddHostedService<UserCreatedEventConsumer>();
builder.Services.AddHostedService<OrderCreatedEventConsumer>();
builder.Services.AddHostedService<OrderCreatedEventConsumer2>();
var app = builder.Build();


app.MapGet("send-unbounded-chanel", async (Channel<UserCreatedEvent> channel) =>
{
    await channel.Writer.WriteAsync(new UserCreatedEvent(1, "John Doe", "john.doe@example.com"));

    Results.Ok();
});

app.MapGet("send-bounded-chanel", async (Channel<OrderCreatedEvent> channel) =>
{
    await channel.Writer.WriteAsync(new OrderCreatedEvent(1, "x", 10));

    Results.Ok();
});


// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();


app.Run();

