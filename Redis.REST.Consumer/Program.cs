using Redis.REST.Consumer.Consumers;
using Redis.Shared;

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

builder.Services.AddHostedService<UserCreatedEventConsumer>();
builder.Services.AddHostedService<UserCreatedEventConsumerWithPattern>();
builder.Services.AddHostedService<UserCreatedEventStreamWithAckConsumer>();
builder.Services.AddHostedService<UserCreatedEventStreamWithNoAckConsumer>();
var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

var summaries = new[]
{
    "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
};

app.MapGet("/weatherforecast", () =>
{
    var forecast = Enumerable.Range(1, 5).Select(index =>
        new WeatherForecast
        (
            DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
            Random.Shared.Next(-20, 55),
            summaries[Random.Shared.Next(summaries.Length)]
        ))
        .ToArray();
    return forecast;
})
.WithName("GetWeatherForecast");

app.Run();

internal record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}
