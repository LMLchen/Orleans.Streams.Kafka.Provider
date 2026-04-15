using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Streams.Kafka;

public class KafkaQueueAdapterReceiver : IQueueAdapterReceiver
{
    private readonly QueueId _queueId;
    private readonly KafkaStreamOptions _options;
    private readonly ILogger _logger;
    private IConsumer<string, byte[]>? _consumer;
    private long _sequenceNumber;

    private KafkaQueueAdapterReceiver(QueueId queueId, KafkaStreamOptions options, ILogger logger)
    {
        _queueId = queueId;
        _options = options;
        _logger = logger;
    }

    public static KafkaQueueAdapterReceiver Create(QueueId queueId, KafkaStreamOptions options, ILogger logger)
    {
        return new KafkaQueueAdapterReceiver(queueId, options, logger);
    }

    public Task Initialize(TimeSpan timeout)
    {
        _consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = _options.BrokerList,
            GroupId = $"{_options.ConsumerGroupId}-{_queueId.GetNumericId()}",
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = true
        }).Build();

        if (_options.Topics.Length > 0)
            _consumer.Subscribe(_options.Topics);

        return Task.CompletedTask;
    }

    public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
    {
        IList<IBatchContainer> batches = new List<IBatchContainer>();
        if (_consumer is null) return Task.FromResult(batches);

        for (var i = 0; i < Math.Min(maxCount, _options.PollBatchSize); i++)
        {
            try
            {
                var result = _consumer.Consume(TimeSpan.FromMilliseconds(_options.PollTimeoutMs));
                if (result?.Message?.Value is null) break;

                var ns = result.Topic;
                var key = result.Message.Key ?? string.Empty;

                // Try to read stream info from headers
                if (result.Message.Headers != null)
                {
                    var nsHeader = result.Message.Headers.FirstOrDefault(h => h.Key == "orleans-stream-namespace");
                    if (nsHeader != null) ns = Encoding.UTF8.GetString(nsHeader.GetValueBytes());

                    var keyHeader = result.Message.Headers.FirstOrDefault(h => h.Key == "orleans-stream-key");
                    if (keyHeader != null) key = Encoding.UTF8.GetString(keyHeader.GetValueBytes());
                }

                var streamId = StreamId.Create(ns, key);
                var batch = new KafkaBatchContainer
                {
                    StreamId = streamId,
                    Payload = result.Message.Value,
                    SequenceNumber = Interlocked.Increment(ref _sequenceNumber)
                };
                batches.Add(batch);
            }
            catch (ConsumeException ex)
            {
                _logger.LogWarning(ex, "Error consuming from Kafka");
                break;
            }
        }

        return Task.FromResult(batches);
    }

    public Task MessagesDeliveredAsync(IList<IBatchContainer> messages) => Task.CompletedTask;

    public Task Shutdown(TimeSpan timeout)
    {
        _consumer?.Close();
        _consumer?.Dispose();
        _consumer = null;
        return Task.CompletedTask;
    }
}
