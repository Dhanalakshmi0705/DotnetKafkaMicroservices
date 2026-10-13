using System.Text.Json;
using Confluent.Kafka;
using OrderProducer.Api.Models;

namespace OrderProducer.Api.Services;

public class KafkaProducerService
{
    private readonly IProducer<string, string> _producer;
    private readonly string _topic = "order-created-events";
    private readonly ILogger<KafkaProducerService> _logger;

    public KafkaProducerService(IConfiguration configuration, ILogger<KafkaProducerService> logger)
    {
        _logger = logger;
        var bootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:9092";

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            EnableDeliveryReports = true,
            LingerMs = 5
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task<bool> PublishOrderCreatedEventAsync(OrderEvent orderEvent)
    {
        try
        {
            var key = orderEvent.OrderId.ToString();
            var val = JsonSerializer.Serialize(orderEvent);

            var message = new Message<string, string>
            {
                Key = key,
                Value = val
            };

            _logger.LogInformation("Publishing OrderEvent {OrderId} to topic {Topic}", orderEvent.OrderId, _topic);

            var deliveryResult = await _producer.ProduceAsync(_topic, message);
            
            _logger.LogInformation("Event published successfully to Partition {Partition} at Offset {Offset}", 
                deliveryResult.Partition.Value, deliveryResult.Offset.Value);

            return true;
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex, "Failed to deliver message to Kafka broker: {Reason}", ex.Error.Reason);
            return false;
        }
    }
}
