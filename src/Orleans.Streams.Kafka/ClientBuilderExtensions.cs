using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;

namespace Orleans.Streams.Kafka;

public static class ClientBuilderExtensions
{
    public static IClientBuilder AddKafkaStreamProvider(
        this IClientBuilder builder,
        string name,
        Action<KafkaStreamOptions> configureOptions)
    {
        builder.Services.AddOptions<KafkaStreamOptions>(name).Configure(configureOptions);
        builder.Services.AddOptions<SimpleQueueCacheOptions>(name).Configure(options =>
        {
            options.CacheSize = 4096;
        });

        builder.AddPersistentStreams(name, KafkaQueueAdapterFactory.Create, stream => { });

        return builder;
    }
}
