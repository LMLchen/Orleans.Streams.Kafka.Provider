using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Streams.Kafka;

public class KafkaQueueAdapterFactory : IQueueAdapterFactory
{
    private readonly string _name;
    private readonly KafkaStreamOptions _options;
    private readonly ILoggerFactory _loggerFactory;
    private readonly SimpleQueueCacheOptions _cacheOptions;
    private HashRingBasedStreamQueueMapper? _mapper;

    public KafkaQueueAdapterFactory(
        string name,
        KafkaStreamOptions options,
        SimpleQueueCacheOptions cacheOptions,
        ILoggerFactory loggerFactory)
    {
        _name = name;
        _options = options;
        _cacheOptions = cacheOptions;
        _loggerFactory = loggerFactory;
    }

    public Task<IQueueAdapter> CreateAdapter()
    {
        var adapter = new KafkaQueueAdapter(_name, _options, _loggerFactory);
        return Task.FromResult<IQueueAdapter>(adapter);
    }

    public IQueueAdapterCache GetQueueAdapterCache()
    {
        return new SimpleQueueAdapterCache(_cacheOptions, _name, _loggerFactory);
    }

    public IStreamQueueMapper GetStreamQueueMapper()
    {
        _mapper ??= new HashRingBasedStreamQueueMapper(
            new HashRingStreamQueueMapperOptions { TotalQueueCount = _options.NumOfQueues },
            _name);
        return _mapper;
    }

    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
    {
        return Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler());
    }

    public static KafkaQueueAdapterFactory Create(IServiceProvider services, string name)
    {
        var optionsMonitor = services.GetRequiredService<IOptionsMonitor<KafkaStreamOptions>>();
        var cacheOptionsMonitor = services.GetRequiredService<IOptionsMonitor<SimpleQueueCacheOptions>>();
        var loggerFactory = services.GetRequiredService<ILoggerFactory>();
        return new KafkaQueueAdapterFactory(
            name,
            optionsMonitor.Get(name),
            cacheOptionsMonitor.Get(name),
            loggerFactory);
    }
}

public class NoOpStreamDeliveryFailureHandler : IStreamFailureHandler
{
    public bool ShouldFaultSubsriptionOnError => false;

    public Task OnDeliveryFailure(GuidId subscriptionId, string streamProviderName, StreamId streamId, StreamSequenceToken? sequenceToken)
        => Task.CompletedTask;

    public Task OnSubscriptionFailure(GuidId subscriptionId, string streamProviderName, StreamId streamId, StreamSequenceToken? sequenceToken)
        => Task.CompletedTask;
}
