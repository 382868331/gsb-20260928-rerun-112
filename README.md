# 工业协议转换网关

这是一个只使用本地连接和虚拟设备的 Go 参考实现。代码不包含真实控制设备地址、驱动或自动重连写入逻辑。

## 首轮协议

- `alpha-fixed-binary`：9 字节大端二进制定长帧，包含地址、功能码、寄存器、有符号 16 位值和 CRC。
- `beta-length-tlv`：小端长度前缀 TLV 帧，包含操作码、标签长度、标签、32 位整数和校验尾。
- 字段映射由 `DefaultMappings()` 显式声明，当前包含温度和压力。

## 转换与拒绝

- 每个端点声明数据类型、字节顺序、工程单位、缩放、偏移和原始范围。
- 转换路径是“原始值 → 工程单位 → 标准单位 → 目标工程单位 → 目标原始值”。
- 超范围、非有限数和不能由目标整数精确表示的值直接返回错误，不做静默截断。
- 压力映射演示 kPa 与 psi 的单位换算；温度支持摄氏度、华氏度和开尔文。

## 请求跟踪

`TraceStore` 可按随机事务 ID 查询一次操作的完整信息：
- 入站 Alpha 帧和出站 Beta 帧的方向、协议和十六进制内容。
- 入站 Beta 响应和出站 Alpha 响应。
- 字段映射、换算前后的值、标准单位值、事件时间线、耗时、状态和错误。
- `Attempts` 始终为 1；写入超时、响应前断连或收到未知状态时保留 `unknown`，网关不会重试成第二次写入。

## 多字段逻辑操作

`Channel.BatchRead` 和 `Channel.BatchWrite` 在一次逻辑调用中处理多个关联字段：

- 批量读始终标记 `snapshot=non-atomic`：每个字段是独立的设备往返，带有各自的 `read_at`、值、错误和报文帧，网关不会把不同设备时刻伪装成原子快照。
- 批量写逐项执行并保留每项 `completed` / `failed` / `unknown` 状态、转换后原始值、尝试次数（恒为 1）和字段级报文证据；部分成功汇总为 `partial`，绝不盲目重发整批，也不重做已成功的项。
- 单位换算不能由目标整数精确表示时，对应项在发送前拒绝（如 1000 kPa → psi），该项没有任何出站写帧，其他项的成功不受影响。
- 写入超时、响应丢失或响应前断连时该项为 `unknown`，不假定写入未发生。调用者使用 `Channel.VerifyWrite` / `Channel.BatchWrite` 结果上的 `VerifyBatchItem` 走只读读回路径：`confirmed` 表示设备当前值与请求一致，`mismatch` 表示设备持有其他值，`unknown` 表示读回本身失败，须稍后再次核实，而不是重新提交写入。
- 批量操作在 `TraceStore` 的 `AddBatch` / `GetBatch` / `ListBatches` 中留有可查询索引。

`SimulatedBetaDevice.ApplyThenDrop` 模拟“设备已落盘但响应丢失”：写副作用真实发生，连接却不返回任何响应，用于核对超时修改不能被当成未发生。

## 预演、确认、执行

条件批量写入支持“先预演、人确认、再执行”的三段式路径，网关不会在确认前修改设备：

- `Channel.PreviewConditionalBatch` 是只读预演：展示每项换算后的目标值（alpha 原始值 → beta 原始值）、预演时刻读到的设备当前值和影响描述，并记录换算规则指纹。预演本身绝不写设备。
- `Channel.ConfirmPreview` 记录人工确认：确认只覆盖操作者当时看到的字段与事实（以预演指纹为凭），指纹不符的迟到确认直接拒绝。
- `Channel.ExecuteConfirmedBatch` 执行前逐项复核：换算规则（映射指纹）、设备当前值（真实读回）、换算目标任一变化，仅把受影响条目标为 `needs_reconfirm` 跳过，其余已确认条目照常执行。
- 并行预演可以针对同一设备的不同字段；共享同一设备约束（同一 beta 标签）时在 `Conflicts` 中报告冲突。
- 执行中响应丢失的条目为 `pending_confirm`，只通过只读回读核实（`ContinueConditionalBatch`），绝不盲目重发。
- 预演、确认、实际生效三类事实都持久化在 `TraceStore`（`GetPreview` / `GetConfirmation` / `GetConditionalBatch`），支持中途重开；`WithdrawConfirmation` 撤回的确认及其预演被墓碑化，迟到请求无法再次启用。

## 本地验证

模拟设备使用 `net.Pipe` 或本地 listener，不连接真实硬件：

```sh
go test ./... -count=1 -race
```

测试覆盖温度缩放边界、压力不可精确表示拒绝、Beta 单字节分段消息、端序、响应前断连、写入超时、事务轨迹查询，以及批量读非原子快照、批量写第二项拒绝的部分成功、响应丢失后只读核实为 `confirmed` 、读回 `mismatch`、预演只读展示换算目标与影响、确认后设备状态或换算规则变化仅受影响条目需重新确认、撤回的确认不可被迟到请求重新启用、并行预演共享约束冲突报告。

## 扩展适配器

新协议实现 `Upstream` 接口，并通过 `NewChannel(..., mappings...)` 显式注册字段映射即可。通道继续通过同一个 `TraceStore` 记录完整来回；新增适配器不得在写入结果不确定时自行重试。

## 带前置条件的批量写入

`Channel.ConditionalBatchWrite` 支持为每一项附带 `WriteCondition`（操作者看到的值与观察时刻）。网关在写入前先只读核对设备当前值，只有与条件一致才执行修改。每项独立报告状态，绝不合并成一个总失败：

- `not_attempted`：条件无法核对（读失败），设备未被触碰。
- `condition_mismatch`：设备当前值与操作者看到的不符，跳过该项修改。
- `applied`：设备已确认生效。
- `pending_confirm`：响应丢失，结果未知，只能通过只读回读核实。
- `conflict`：后续核对发现设备自行变化，需要操作者裁决。
- `reverted`：恢复已把字段写回修改前的值。

`ConditionEvidence` 在跨单位换算后仍保留原字段来源、操作者看到的 alpha 原始值、换算后的 beta 原始值、核对时设备实际值以及观察与核对两个时刻。批次结果通过 `TraceStore.AddConditionalBatch` / `GetConditionalBatch` 持久化，重开会话后可继续核对不确定项。

部分生效批次的后续处理：

- `Channel.ContinueConditionalBatch`：继续未完成项。`pending_confirm` 项只读回读——确认则转为 `applied`，发现设备持有其他值（响应丢失与设备自行变化同时出现）则转为 `conflict`，绝不盲目重复写入；`not_attempted` / `condition_mismatch` 项按当前设备真实值重新核对条件后再写。
- `Channel.RollbackConditionalBatch`：按明确规则恢复已改项——写回条件记录的修改前的值。恢复同样先读回设备真实值，仅当设备仍持有本批次写入的值时才回退；设备已自行变化则标记 `conflict` 并保持不动。
