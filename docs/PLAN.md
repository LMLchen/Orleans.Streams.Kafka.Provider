# 执行计划

## Step 1: 项目脚手架
- 创建 `Orleans.Streams.Kafka.sln` 解决方案
- 创建 `src/Orleans.Streams.Kafka/Orleans.Streams.Kafka.csproj`（含 NuGet 元数据）
- 创建 `LICENSE` (MIT)
- 创建 `README.md`

## Step 2: 配置与选项
- `KafkaStreamOptions.cs` — Kafka 连接和行为配置

## Step 3: 核心实现
- `KafkaQueueAdapter.cs` — IQueueAdapter 实现（Producer 端）
- `KafkaQueueAdapterReceiver.cs` — IQueueAdapterReceiver 实现（Consumer 端）
- `KafkaQueueAdapterFactory.cs` — IQueueAdapterFactory 实现
- `KafkaBatchContainer.cs` — IBatchContainer 实现（消息容器）
- `KafkaStreamFailureHandler.cs` — IStreamFailureHandler 实现

## Step 4: 扩展方法
- `SiloBuilderExtensions.cs` — Silo 端注册扩展
- `ClientBuilderExtensions.cs` — Client 端注册扩展

## Step 5: NuGet 发布配置
- 确保 csproj 包含完整 NuGet 包属性
- 创建 `.github/workflows/nuget-publish.yml` CI/CD
