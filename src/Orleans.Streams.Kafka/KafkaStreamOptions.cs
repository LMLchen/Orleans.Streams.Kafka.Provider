namespace Orleans.Streams.Kafka;

public class KafkaStreamOptions
{
    public string BrokerList { get; set; } = "localhost:9092";
    public string ConsumerGroupId { get; set; } = "orleans";
    public string[] Topics { get; set; } = [];
    public int NumOfQueues { get; set; } = 8;
    public int PollTimeoutMs { get; set; } = 100;
    public int PollBatchSize { get; set; } = 100;
}
