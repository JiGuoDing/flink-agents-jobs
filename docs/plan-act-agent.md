# Plan-Act 售后 Agent

本文说明 `nju.jgd.planact` 中可复用的固定计划 Plan-Act 能力，以及
`nju.jgd.job.PlanActSupportJob` 这个可运行示例。

## 1. 它解决什么问题

Plan-Act 把一次 Agent 任务拆成三个阶段：

1. **Plan**：LLM 先输出完整计划，计划中的每一步必须调用已注册 Tool。
2. **Act**：Agent 严格按计划顺序执行 Tool，一次只执行一步。
3. **Synthesize**：根据原始目标、计划和 Tool 结果调用 LLM 生成最终回答。

与 ReAct 的区别是：ReAct 在每一步后让模型重新决定下一步；Plan-Act 第一版先固定整份计划，
执行期间不重规划。因此它更适合步骤可预先分解、需要可审计计划的任务。

## 2. 可复用 API

### 输入与输出

```java
PlanActInput input = new PlanActInput(
        "task-1",
        "查询订单 ORD-1001 的物流状态",
        Map.of("userId", "u-1001"),
        Map.of("requestId", "req-1"));

PlanActResult result = ...;
```

- `PlanActInput.context` 和 `metadata` 使用 `Map<String,String>`，便于稳定地参与 Flink 序列化和
  checkpoint。
- `PlanActResult` 包含计划、每步输出、最终回答、模型名和总耗时，不只是一个字符串。

### 构建 Agent

```java
PlanActAgent agent = PlanActAgent.builder()
        .chatModel(openAIChatModelDescriptor)
        .tool(PlanActTool.fromStaticMethod(
                OrderTools.class.getMethod("lookupOrderStatus", String.class)))
        .tool(PlanActTool.fromStaticMethod(
                OrderTools.class.getMethod(
                        "checkRefundEligibility", String.class, String.class)))
        .maxSteps(8)
        .plannerInstructions("你是售后任务规划器，只输出规定格式的 JSON。")
        .synthesisInstructions("你是中文售后助手，只能使用 Tool 结果中的事实。")
        .build();
```

`PlanActTool.fromStaticMethod` 会读取方法上的 `@Tool`、`@ToolParam` 注解，生成 Tool 名称、
描述和输入 JSON Schema。Tool 名称默认使用 Java 方法名，因此计划中的 `tool` 字段必须和它一致。

### 计划协议

Planner 必须返回：

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

约束：

- `steps` 必须是 1 到 `maxSteps` 个元素。
- `id` 必须非空且在本次计划内唯一。
- `tool` 必须是已注册 Tool 的精确名称。
- `arguments` 必须是 JSON object；可以省略，表示空参数。
- 不允许在计划中加入“最终回答”步骤，最终回答由 Synthesis 阶段完成。
- Planner 返回 Markdown JSON 代码块时，解析器会剥离代码块后再解析；其他格式直接失败。

## 3. Flink Agents 事件流

```text
InputEvent
  -> plan Action
       调用 planner Chat Model
       解析并校验计划
       保存 PlanActExecution 到 Sensory Memory
  -> PlanReadyEvent(stepIndex=0)
  -> advance Action
       执行当前 Tool
       保存 StepResult
  -> StepFinishedEvent(stepIndex=next)
  -> advance Action
       ...
  -> advance Action(stepIndex=planSize)
       调用 synthesis Chat Model
  -> OutputEvent<PlanActResult>
```

关键实现点：

- Chat Model 不绑定 Tool；模型负责输出计划文本，Tool 由 Plan-Act 引擎显式执行。
- 计划状态存放在 `PlanActStateStore.STATE_PATH`，当前值为 `planact.execution`。
- 每个 key 同时只允许一个活跃执行；`taskId` 不一致会立即失败。
- 内部事件携带 `taskId` 和 `stepIndex`。旧事件会被忽略，超前事件会报错，用于避免 checkpoint
  恢复时重复执行步骤。
- 计划为空、JSON 非法、Tool 不存在、Tool 调用失败或最终回答为空都会抛出
  `PlanActException`，不会生成伪造回答。

## 4. 运行示例

1. 复制 `config/support-agent.properties.example` 为
   `config/support-agent.properties`。
2. 填写 `llm.base-url`、`llm.api-key` 和 `llm.model`。
3. 确保 `planact.max-steps` 在 1 到 20 之间，默认是 8。
4. 使用 IntelliJ 运行 `nju.jgd.job.PlanActSupportJob`，Program arguments 填写
   `--config config/support-agent.properties`。
5. 或提交到 Flink：

```bash
$FLINK_HOME/bin/flink run \
  -c nju.jgd.job.PlanActSupportJob \
  /path/to/flink-jobs-1.0-SNAPSHOT.jar \
  --config /path/to/support-agent.properties
```

Flink 集群仍需配置：

```yaml
env.java.opts.all: >-
  --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED
execution.runtime-mode: STREAMING
```

## 5. 测试与限制

自动化测试不访问真实模型：

- `PlanActPlanParserTest` 验证计划 JSON、代码块、空计划、未知 Tool、重复步骤和参数类型。
- `PlanActEngineTest` 验证顺序推进和非法状态转换。
- `PlanActStateStoreTest` 验证状态 JSON round-trip。
- `PlanActAgentTest` 验证 builder 注册模型、Tool 和 Action。
- `PlanActAgentWorkflowTest` 使用脚本化 `RunnerContext` 跑完整
  Plan -> Tool -> Synthesis 事件链。

当前限制：

- 只支持固定顺序计划，不支持执行中重规划、DAG 或并行步骤。
- 每个 key 同时只允许一个活跃 Plan-Act 执行。
- Flink checkpoint 可以恢复计划和步骤状态，但外部 Tool 调用不保证 exactly-once；有副作用的
  Tool 需要业务幂等键或去重。
- 示例 Job 不接入现有短期记忆、会话缓存和用户历史；这些可以后续在 Planner 输入或 Tool 层扩展。