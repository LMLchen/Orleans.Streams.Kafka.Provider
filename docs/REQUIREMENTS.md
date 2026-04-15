# Orleans.Streams.Kafka.Provider 需求文档

## 项目概述
基于 Microsoft Orleans 的 Kafka Stream Provider，允许 Orleans Grain 通过 Kafka 作为底层传输层进行流式消息的生产和消费。

## 功能需求

### 核心功能
1. **Kafka Producer Adapter** — 将 Orleans Stream 消息发布到 Kafka Topic
2. **Kafka Consumer Adapter** — 从 Kafka Topic 消费消息并投递到 Orleans Stream
3. **Queue Adapter Factory** — Orleans Stream 的队列适配器工厂，负责创建 Producer/Consumer
4. **Silo 扩展方法** — 提供 `AddKafkaStreamProvider` 扩展方法，简化配置
5. **Client 扩展方法** — 提供客户端连接 Kafka Stream 的扩展方法

### 配置需求
- Kafka Broker 地址列表
- Consumer Group ID
- Topic 前缀/映射
- 序列化方式（默认 JSON）
- 消费者轮询间隔
- 批量大小

### 非功能需求
- 支持 .NET 8+
- 依赖 `Confluent.Kafka` 作为 Kafka 客户端
- 依赖 `Microsoft.Orleans.Streaming` 作为 Orleans 流框架
- 可发布到 NuGet.org
- 包含完整的 NuGet 包元数据（作者、许可证、描述等）

## 技术架构
```
Orleans Grain
    ↓ (IAsyncStream<T>)
KafkaQueueAdapterFactory
    ↓
KafkaQueueAdapter (Producer) / KafkaQueueAdapterReceiver (Consumer)
    ↓
Confluent.Kafka (IProducer / IConsumer)
    ↓
Apache Kafka Broker
```

## NuGet 包信息
- **Package ID**: Orleans.Streams.Kafka.Provider
- **Target Framework**: net8.0
- **License**: MIT
- **Dependencies**:
  - Microsoft.Orleans.Streaming >= 8.0.0
  - Confluent.Kafka >= 2.3.0
