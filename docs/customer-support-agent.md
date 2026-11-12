# 售后助手 Flink Agent

## 目标

这个示例不是只把字符串转成大写的 Mock Agent，而是完整演示：

1. 真实调用 OpenAI 兼容 Chat Completions API。
2. 使用显式 Flink Keyed State 记录用户活动。
3. 使用 Flink Agents 短期记忆保存多轮会话。
4. 让模型通过 Flink Agents 自动工具调用查询订单和退款规则。
5. 使用 Flink State 实现同一用户跨会话的精确匹配缓存。
6. 启用 checkpoint，支持状态恢复。

## 数据流

```text
SupportRequestSource
  -> keyBy(userId)
  -> UserSessionStateFunction
       ValueState<UserSessionState>
  -> CustomerSupportAgent
       short-term memory (Flink MapState)
       exact-match response cache
       OpenAI Chat Completions
       built-in tool loop
         -> lookupOrderStatus
         -> checkRefundEligibility
  -> SupportResponse
  -> print
```

## 显式 Flink 状态

`UserSessionStateFunction` 是原生 Flink `KeyedProcessFunction`，其 `ValueState` 包含：

- `firstSeenAt`
- `lastSeenAt`
- `requestCount`
- `lastSessionId`

该状态用于判断回访用户并生成 `requestSequence`。状态 TTL 与 Agent 短期记忆 TTL
使用同一个配置上限。

## Agent 记忆

Agent Operator 按 `userId` 分区。一个用户的所有 session 共享同一个短期记忆根节点，但历史按 session 隔离：

```text
sessions.<safe-session-id>.history
sessions.<safe-session-id>.turnCount
```

每轮完成后同时写入 USER 和 ASSISTANT 消息，只保留最近 8 条。Prompt 会带上
历史记录、回访状态和当前问题，因此模型可以理解“刚才那个订单”这类指代。

## 精确匹配缓存

缓存只处理 `cacheable=true` 的独立问题。缓存键由以下内容计算 SHA-256：

```text
规范化问题 + 模型名 + Prompt 版本
```

缓存路径：

```text
cache.<sha256>.answer
cache.<sha256>.model
cache.<sha256>.createdAt
```

因为 Agent 按 `userId` 分区，同一用户的不同 session 可以命中同一个缓存条目，不同用户之间不会共享。
缓存命中仍会写入当前 session 的会话历史，然后跳过模型调用。缓存存在逻辑 TTL，过期后重新推理并覆盖。

## 工具

`OrderTools` 使用内置确定性数据：

- `ORD-1001`：运输中
- `ORD-1002`：已签收
- 其他订单：明确返回 `found=false`

模型必须先查询订单事实，再描述物流或退款结论。工具结果进入 Flink Agents 的内置
工具调用循环，模型会继续推理并生成最终中文回答。

## 配置

默认读取：

```text
config/support-agent.properties
```

也可以通过：

```text
--config <path>
```

指定其他文件。配置项说明：

| Key | 说明 |
| --- | --- |
| `llm.base-url` | OpenAI 兼容 API 的 `/v1` 基础地址，必须填写 |
| `llm.api-key` | API Key，必须填写 |
| `llm.model` | 模型名 |
| `llm.temperature` | 0.0 到 2.0 |
| `llm.max-tokens` | 单次回答最大 token 数 |
| `llm.timeout-seconds` | HTTP 超时秒数 |
| `llm.max-retries` | OpenAI SDK 重试次数 |
| `agent.async-threads` | Flink Agents 异步线程数 |
| `agent.max-in-flight-records` | 每个算子子任务最大在途输入数 |
| `agent.memory-ttl-seconds` | 短期记忆和用户状态 TTL |
| `cache.ttl-seconds` | 逻辑缓存 TTL，不得大于记忆 TTL |
| `job.parallelism` | Job 默认并行度 |
| `job.checkpoint.interval-ms` | checkpoint 间隔 |
| `job.checkpoint.min-pause-ms` | checkpoint 最小间隔 |
| `job.checkpoint.timeout-ms` | checkpoint 超时 |
| `job.checkpoint.dir` | checkpoint 存储；为空时使用 Flink 默认值 |
| `demo.records-per-second` | 本地 Source 生成速率 |

启动时会检查必填项和数值范围。API Key 不会出现在异常或日志中。

## 手工验收

填入配置后运行 Job，重点观察：

1. 第一条订单问题触发 `lookupOrderStatus` 工具。
2. 同一 session 的后续问题能够引用上一轮订单。
3. 同一用户不同 session 的重复 FAQ 出现 `cacheHit=true`。
4. 不同用户不会复用该缓存。
5. Flink Web UI 中 checkpoint 正常完成。
6. 状态恢复后短期记忆和缓存仍然存在。

## 限制

- 这是本地有界演示，不包含 Kafka、数据库或生产 Sink。
- 缓存是精确匹配，不是语义缓存。
- 没有长期记忆、Embedding、向量库或 MCP。
- Flink checkpoint 保证状态恢复，不保证外部 LLM 调用 exactly-once。
- 生产环境应使用共享 checkpoint 存储，并评估模型调用重放成本。
