using System.Text.Json;
using Redis.Shared;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

builder.Services.AddSingleton<RedisService>(sp =>
{
    var host = builder.Configuration["RedisOption:Host"]!;
    var port = builder.Configuration["RedisOption:Port"]!;
    var password = builder.Configuration["RedisOption:Password"]!;

    var redisService = new RedisService(host, port, password);

    redisService.Init().Wait();
    return redisService;
});

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();


app.MapGet("/pub-sub-send", async (RedisService redisService) =>
{
    var subscriber = redisService.Subscriber;

    var userCreatedEvent = new UserCreatedEvent("ahmet", "ahmet@outlook.com");

    var userCreatedEventAsJson = JsonSerializer.Serialize(userCreatedEvent);


    await subscriber.PublishAsync("user_created_event_channel", userCreatedEventAsJson);
    return Results.Ok("Message sent");
});


app.MapGet("/pub-sub-send-with-pattern", async (RedisService redisService) =>
{
    var subscriber = redisService.Subscriber;

    var userCreatedEvent = new UserCreatedEvent("ahmet", "ahmet@outlook.com");

    var userCreatedEventAsJson = JsonSerializer.Serialize(userCreatedEvent);


    await subscriber.PublishAsync("info.warning.critical", userCreatedEventAsJson);
    return Results.Ok("Message sent");
});

app.MapGet("/redis-stream-send-no-ack", async (RedisService redisService) =>
{
    var db = redisService.GetDatabase;


    var values = new[]
        {
            new NameValueEntry("userId", "1"),
            new NameValueEntry("userName", "ahmet"),
            new NameValueEntry("email", "ahmet@outlook.com"),
        };
        var messageId = await db.StreamAddAsync("user_created_stream", values, flags: CommandFlags.FireAndForget);

        return Results.Ok(messageId.ToString());
});

app.MapGet("/redis-stream-send-yes-ack", async (RedisService redisService) =>
{
    var db = redisService.GetDatabase;


    var values = new[]
        {
            new NameValueEntry("userId", "1"),
            new NameValueEntry("userName", "ahmet"),
            new NameValueEntry("email", "ahmet@outlook.com"),
        };
    var messageId = await db.StreamAddAsync("user_created_stream", values, flags: CommandFlags.DemandMaster);

    return Results.Ok(messageId.ToString());
});
app.Run();
