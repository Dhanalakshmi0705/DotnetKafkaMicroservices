using OrderConsumer.Service.Workers;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<KafkaOrderConsumerWorker>();

var host = builder.Build();
host.Run();
