using System.Text.Json;
using Confluent.Kafka;

namespace OrderConsumer.Service.Workers;

public class KafkaOrderConsumerWorker : BackgroundService
{
    private readonly ILogger<KafkaOrderConsumerWorker> _logger;
    private readonly string _topic = "order-created-events";
    private readonly ConsumerConfig _consumerConfig;

    public KafkaOrderConsumerWorker(IConfiguration configuration, ILogger<KafkaOrderConsumerWorker> logger)
    {
        _logger = logger;
        var bootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:9092";

        _consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = "order-processing-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        using var consumer = new ConsumerBuilder<string, string>(_consumerConfig).Build();
        consumer.Subscribe(_topic);

        _logger.LogInformation("Kafka Worker Consumer started listening on topic: {Topic}", _topic);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(stoppingToken);

                if (consumeResult?.Message == null) continue;

                _logger.LogInformation("Received Message Key: {Key} | Value: {Value}", 
                    consumeResult.Message.Key, consumeResult.Message.Value);

                await ProcessOrderAsync(consumeResult.Message.Value);

                consumer.Commit(consumeResult);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Consumer shutting down gracefully...");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing Kafka message");
            }
        }

        consumer.Close();
    }

    private Task ProcessOrderAsync(string messageValue)
    {
        _logger.LogInformation("[BUSINESS LOGIC] Processing order transaction asynchronously...");
        return Task.CompletedTask;
    }
}
