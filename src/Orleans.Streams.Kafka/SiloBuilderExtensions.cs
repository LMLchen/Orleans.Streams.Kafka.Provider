using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;

namespace Orleans.Streams.Kafka;

public static class SiloBuilderExtensions
{
    public static ISiloBuilder AddKafkaStreamProvider(
        this ISiloBuilder builder,
        string name,
        Action<KafkaStreamOptions> configureOptions)
    {
        builder.AddPersistentStreams(name, KafkaQueueAdapterFactory.Create, stream =>
        {
            stream.Configure<KafkaStreamOptions>(ob => ob.Configure(configureOptions));
            stream.Configure<SimpleQueueCacheOptions>(ob => ob.Configure(options =>
            {
                options.CacheSize = 4096;
            }));
            stream.Configure<HashRingStreamQueueMapperOptions>(ob => ob.Configure(options =>
            {
                // Will be overridden by NumOfQueues in factory
            }));
        });
        return builder;
    }
}
