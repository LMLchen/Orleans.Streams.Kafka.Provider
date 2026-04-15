using System.Text.Json;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Streams.Kafka;

[GenerateSerializer]
public class KafkaBatchContainer : IBatchContainer
{
    [Id(0)] public StreamId StreamId { get; set; }
    [Id(1)] public byte[] Payload { get; set; } = [];
    [Id(2)] public long SequenceNumber { get; set; }
    [Id(3)] public Dictionary<string, object>? RequestContext { get; set; }

    public StreamSequenceToken SequenceToken => new EventSequenceTokenV2(SequenceNumber);

    public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
    {
        var item = JsonSerializer.Deserialize<T>(Payload)!;
        return [Tuple.Create(item, SequenceToken)];
    }

    public bool ImportRequestContext()
    {
        if (RequestContext is null) return false;
        foreach (var kvp in RequestContext)
            Runtime.RequestContext.Set(kvp.Key, kvp.Value);
        return true;
    }

    public static KafkaBatchContainer Create<T>(StreamId streamId, T item, long sequenceNumber)
    {
        return new KafkaBatchContainer
        {
            StreamId = streamId,
            Payload = JsonSerializer.SerializeToUtf8Bytes(item),
            SequenceNumber = sequenceNumber,
        };
    }
}
