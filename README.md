# Orleans.Streams.Kafka

A Kafka Stream Provider for Microsoft Orleans that enables Grains to produce and consume messages via Apache Kafka.

## Installation

```bash
dotnet add package Orleans.Streams.Kafka
```

## Quick Start

### Silo Configuration

```csharp
siloBuilder.AddKafkaStreamProvider("Kafka", options =>
{
    options.BrokerList = "localhost:9092";
    options.ConsumerGroupId = "orleans-consumers";
    options.Topics = new[] { "my-topic" };
});
```

### Grain Usage

```csharp
public class ProducerGrain : Grain, IProducerGrain
{
    public async Task Produce(string message)
    {
        var streamProvider = this.GetStreamProvider("Kafka");
        var stream = streamProvider.GetStream<string>("my-topic", "key");
        await stream.OnNextAsync(message);
    }
}

public class ConsumerGrain : Grain, IConsumerGrain, IAsyncObserver<string>
{
    public override async Task OnActivateAsync(CancellationToken ct)
    {
        var streamProvider = this.GetStreamProvider("Kafka");
        var stream = streamProvider.GetStream<string>("my-topic", "key");
        await stream.SubscribeAsync(this);
    }

    public Task OnNextAsync(string item, StreamSequenceToken? token = null)
    {
        Console.WriteLine($"Received: {item}");
        return Task.CompletedTask;
    }

    public Task OnCompletedAsync() => Task.CompletedTask;
    public Task OnErrorAsync(Exception ex) => Task.CompletedTask;
}
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `BrokerList` | `localhost:9092` | Kafka broker addresses |
| `ConsumerGroupId` | `orleans` | Consumer group ID |
| `Topics` | `[]` | Topics to subscribe |
| `PollTimeoutMs` | `100` | Consumer poll timeout (ms) |
| `NumOfQueues` | `8` | Number of queue partitions |

## License

MIT
