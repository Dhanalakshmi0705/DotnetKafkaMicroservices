namespace OrderProducer.Api.Models;

public record OrderEvent(
    Guid OrderId,
    string CustomerId,
    decimal Amount,
    string Currency,
    DateTime CreatedAtUtc
);
