using Kafka.Rest.Producer.Serrvices;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
builder.Services.AddSingleton<KafkaService>();
var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
    app.MapScalarApiReference();
}

app.UseHttpsRedirection();

app.MapPost("/api/kafka/create-topic", async (KafkaService kafkaService) =>
{
    kafkaService.TopicName = "my-topic";
    await kafkaService.CreateTopicAsync();
    return Results.Ok();
});
app.MapPost("/api/kafka/send-simple-message", async (KafkaService kafkaService) =>
{
    kafkaService.TopicName = "my-topic";
    await kafkaService.SendAtMostOnceSimpleMessage();
    return Results.Ok();
});
app.MapPost("/api/kafka/send-complex-message", async (KafkaService kafkaService) =>
{
    kafkaService.TopicName = "order.created.event-topic";
    await kafkaService.CreateTopicAsync();
    await kafkaService.SendAtMostOnceComplexMessage();
    return Results.Ok();
});
app.Run();

