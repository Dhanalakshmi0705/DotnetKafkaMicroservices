using OrderProducer.Api.Models;
using OrderProducer.Api.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddSingleton<KafkaProducerService>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapPost("/api/orders", async (CreateOrderRequest request, KafkaProducerService producer) =>
{
    var orderEvent = new OrderEvent(
        OrderId: Guid.NewGuid(),
        CustomerId: request.CustomerId,
        Amount: request.Amount,
        Currency: request.Currency ?? "INR",
        CreatedAtUtc: DateTime.UtcNow
    );

    var success = await producer.PublishOrderCreatedEventAsync(orderEvent);

    return success 
        ? Results.Accepted($"/api/orders/{orderEvent.OrderId}", orderEvent) 
        : Results.StatusCode(500);
})
.WithName("CreateOrder")
.WithOpenApi();

app.Run();

public record CreateOrderRequest(string CustomerId, decimal Amount, string? Currency);
