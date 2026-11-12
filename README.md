# Flink Agents Jobs

Java 21 + Apache Flink Agents 的流式 Agent Job 示例工程。

## Project structure

```text
src/main/java/nju/jgd/
├── job/
│   ├── AgentJob.java
│   ├── CustomerSupportAgentJob.java
│   ├── PlanActSupportJob.java
│   └── config/SupportJobConfig.java
├── operator/
│   ├── agent/
│   │   ├── EchoAgent.java
│   │   └── CustomerSupportAgent.java
│   ├── source/
│   │   ├── DemoAgentRequestSource.java
│   │   ├── PlanActSupportRequestSource.java
│   │   └── SupportRequestSource.java
│   └── state/UserSessionStateFunction.java
├── planact/
│   ├── PlanActAgent.java
│   ├── PlanActEngine.java
│   ├── PlanActPlanParser.java
│   ├── PlanActStateStore.java
│   └── PlanActTool.java
├── tool/support/OrderTools.java
├── support/
│   ├── CacheKeys.java
│   ├── CacheTtl.java
│   ├── ConversationHistory.java
│   └── SupportMemory.java
└── model/
    ├── SupportRequest.java
    ├── SupportResponse.java
    ├── UserSessionState.java
    ├── CachedAnswer.java
    ├── AgentRequest.java
    └── AgentResponse.java
```

`job` 只负责组装和提交拓扑；Operator 按类型独立放在 `operator/` 下；本地工具、
记忆/缓存辅助类和业务数据体分别放在独立包中。

## Version baseline

- Apache Flink: `2.3.0`
- Apache Flink Agents: `0.4.0`
- Java: `21`
- Maven: `3.10.0` through the Maven Wrapper
- Python: `3.12`, only when a job uses PyFlink or Python Actions/Resources

## Build

```powershell
$env:JAVA_HOME = 'C:\software\dragonwell-21.0.12.0.12+8-GA'
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
.\mvnw.cmd clean package
```

生成的 Job JAR：

```text
target/flink-jobs-1.0-SNAPSHOT.jar
```

Shade JAR 默认主类是 `nju.jgd.job.CustomerSupportAgentJob`。原 Echo 示例仍保留，可通过
`-c nju.jgd.job.AgentJob` 运行。

Flink 和 Flink Agents 依赖使用 `provided` scope，不应把 Flink Agents distribution
JAR 打进业务 JAR。

## Customer support agent

新 Job 演示以下完整链路：

```text
bounded demo source
  -> keyBy(userId)
  -> explicit ValueState<UserSessionState>
  -> Flink Agents short-term memory
  -> exact-match response cache
  -> OpenAI-compatible Chat Completions
  -> automatic tool calling
  -> print sink
```

核心能力：

- 显式 Flink Keyed State：`UserSessionStateFunction` 保存用户首次访问、最近访问、请求总数和上一会话。
- Flink Agents 短期记忆：按 `userId` 分区，按 `sessionId` 保存最近 8 条会话消息。
- 跨会话缓存：同一用户的多个 session 共享精确匹配缓存，不跨用户泄漏。
- 真实推理：通过 `VLLMChatModelConnection` 和 `ChatRequestEvent` 调用 vLLM 模型。
- 自动工具：模型可调用 `lookupOrderStatus` 和 `checkRefundEligibility`。
- Checkpoint：启用 EXACTLY_ONCE checkpoint，缓存和记忆随作业状态恢复。
- 失败语义：模型或工具失败直接抛出，不返回伪造答案。

详细说明见 [docs/customer-support-agent.md](docs/customer-support-agent.md)。

## Plan-Act support agent

`PlanActSupportJob` 使用同一套售后订单数据，演示固定的 Plan-and-Execute 流程：

```text
support request
  -> keyBy(userId)
  -> PlanActInput
  -> Planner LLM (tool plan JSON)
  -> execute one Tool per internal event
  -> Synthesis LLM
  -> PlanActResult（计划、步骤结果、最终回答）
```

核心能力：

- `nju.jgd.planact.PlanActAgent` 是通用工具型 Plan-Act Agent，通过 builder 注册模型和 Tool。
- 计划必须引用已注册 Tool，步骤数受 `planact.max-steps` 限制，默认 8，最大 20。
- 计划与执行状态以 JSON 写入 Sensory Memory，每个 key 同时只允许一个活跃执行。
- 当前版本固定顺序执行，不支持执行中重规划、DAG 并行和响应缓存。
- 计划解析、Tool 不存在、Tool 失败或最终回答为空时直接失败，不生成伪造答案。

计划 JSON 约定：

```json
{
  "steps": [
    {
      "id": "step-1",
      "tool": "lookupOrderStatus",
      "arguments": {"orderId": "ORD-1001"},
      "purpose": "查询订单状态"
    }
  ]
}
```

详细复用说明见 [docs/plan-act-agent.md](docs/plan-act-agent.md)。

### Configure

编辑：

```text
config/support-agent.properties
```

至少填写：

```properties
llm.base-url=http://114.212.135.74:18080/v1
llm.api-key=your-api-key
llm.model=qwen3-0.6b
# Qwen3 on vLLM: disable thinking mode.
llm.additional-kwargs={"chat_template_kwargs":{"enable_thinking":false}}
```

`config/support-agent.properties` 已加入 `.gitignore`，不会提交真实密钥。
`config/support-agent.properties.example` 可作为模板。

### Run locally with IntelliJ

1. 使用 `dragonwell-21` 项目 SDK。
2. 激活 Maven profile `ide`。
3. 运行 `nju.jgd.job.CustomerSupportAgentJob`。
4. Program arguments 填 `--config config/support-agent.properties`。
5. 勾选 **Add dependencies with provided scope to classpath**。
6. VM option 增加：

```text
--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED
```

### Run on Flink 2.3.0

将匹配版本的 Flink Agents distribution JAR 放入 `$FLINK_HOME/lib`：

```text
flink-agents-dist-flink-2.3-0.4.0.jar
```

Flink 配置：

```yaml
env.java.opts.all: >-
  --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED
execution.runtime-mode: STREAMING
```

提交默认 Job：

```bash
$FLINK_HOME/bin/flink run \
  /path/to/flink-jobs-1.0-SNAPSHOT.jar \
  --config /path/to/support-agent.properties
```

提交旧 Echo Job：

```bash
$FLINK_HOME/bin/flink run \
  -c nju.jgd.job.AgentJob \
  /path/to/flink-jobs-1.0-SNAPSHOT.jar
```

提交 Plan-Act Job：

```bash
$FLINK_HOME/bin/flink run \
  -c nju.jgd.job.PlanActSupportJob \
  /path/to/flink-jobs-1.0-SNAPSHOT.jar \
  --config /path/to/support-agent.properties
```

## Optional Python 3.12 environment

Only create this environment when a job uses PyFlink, Python Actions or Resources,
Python MCP, or another Python-based integration.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install flink-agents==0.4.0 apache-flink==2.3.0
export PYTHONPATH=$(python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')
```

For a PyFlink job, copy Flink's Python JAR into `lib`:

```bash
cp $FLINK_HOME/opt/flink-python-2.3.0.jar $FLINK_HOME/lib/
```

Submit with:

```bash
$FLINK_HOME/bin/flink run -py /path/to/job.py
```

## Implementation notes

- Flink checkpoint 可以恢复 Keyed State、Agent 记忆和缓存，但不保证外部 LLM 调用 exactly-once。
- 若外部调用有副作用，应增加幂等键或配置 Action State Store。
- 当前演示使用有界 DataGeneratorSource 和 print Sink；生产环境应替换为 Kafka/Table Source 与持久化 Sink。
- `agent.max-in-flight-records=1` 用于演示顺序和缓存命中，生产环境应结合 key 级顺序约束调整。
