using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Streams.Kafka;

public class KafkaQueueAdapter : IQueueAdapter
{
    private readonly KafkaStreamOptions _options;
    private readonly ILogger _logger;
    private readonly IProducer<string, byte[]> _producer;

    public string Name { get; }
    public bool IsRewindable => false;
    public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

    public KafkaQueueAdapter(string name, KafkaStreamOptions options, ILoggerFactory loggerFactory)
    {
        Name = name;
        _options = options;
        _logger = loggerFactory.CreateLogger<KafkaQueueAdapter>();
        _producer = new ProducerBuilder<string, byte[]>(new ProducerConfig
        {
            BootstrapServers = options.BrokerList
        }).Build();
    }

    public async Task QueueMessageBatchAsync<T>(StreamId streamId, IEnumerable<T> events, StreamSequenceToken? token, Dictionary<string, object> requestContext)
    {
        var topic = streamId.GetNamespace() ?? "default";
        var key = streamId.GetKeyAsString();

        foreach (var evt in events)
        {
            var payload = JsonSerializer.SerializeToUtf8Bytes(evt);
            var message = new Message<string, byte[]>
            {
                Key = key ?? string.Empty,
                Value = payload,
                Headers = new Headers
                {
                    { "orleans-stream-namespace", System.Text.Encoding.UTF8.GetBytes(topic!) },
                    { "orleans-stream-key", System.Text.Encoding.UTF8.GetBytes(key ?? string.Empty) }
                }
            };

            await _producer.ProduceAsync(topic, message);
            _logger.LogDebug("Produced message to topic {Topic} with key {Key}", topic, key);
        }
    }

    public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
    {
        return KafkaQueueAdapterReceiver.Create(queueId, _options, _logger);
    }
}
