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

## 带前置条件的批量写入

`Channel.ConditionalBatchWrite` 在每项写入前先做只读核对：只有设备当前值仍等于操作者看到的值（`Precondition` 中的 alpha 原始值与读取时刻）时才执行修改。每项独立报告状态，绝不合并成一个总失败：

- `completed`：条件符合且写入已被确认，已生效。
- `unknown`：响应丢失或断连，仍待确认；网关假定写入可能已发生，不会重发。
- `condition_mismatch`：设备当前值与操作者看到的不符，未发送任何写帧。
- `not_attempted`：前置核对本身读不到设备值，写入未尝试。
- `conflict`：后续处理时发现设备被外部改动，网关拒绝盲目覆盖。

每项保留换算前后的值、条件来源值与读取时刻、改前设备值（`PreValue`）和报文证据；跨单位换算不会抹掉原字段来源。

## 部分生效批次的后续处理

- `Channel.ContinueBatch`：只处理未完成项。`unknown` 项先走只读读回，确认一致则标记完成（不重发写）；若设备持有其他值（回应丢失 + 设备自行变化），标记 `conflict`，绝不盲目重复写入。`not_attempted` / `condition_mismatch` 项重新做完整条件核对，仅当设备当前值符合条件时才写入。
- `Channel.RevertBatch`：按明确规则 `RevertToPreBatchValue` 恢复已改项，恢复目标只能是本批改前记录的设备真实值；恢复前再次读回核对，设备仍持有本批写入的值才恢复，已被外部改动的项标记 `conflict` 并保持原样。

## 不确定项的持久化

`BatchJournal`（`LoadBatchJournal(path)`，挂到 `Channel.Journal`）把每个条件批次落盘：字段、请求的 alpha 原始值、实际发送的 beta 原始值、前置条件来源值与读取时刻、改前设备值和每项状态。网关重启后从日志重新加载记录，用 `VerifyConditionalItem` 继续只读核对 `unknown` 项，不需要重新提交写入。

## 本地验证

模拟设备使用 `net.Pipe` 或本地 listener，不连接真实硬件：

```sh
go test ./... -count=1 -race
```

测试覆盖温度缩放边界、压力不可精确表示拒绝、Beta 单字节分段消息、端序、响应前断连、写入超时、事务轨迹查询，以及批量读非原子快照、批量写第二项拒绝的部分成功、响应丢失后只读核实为 `confirmed` 和读回 `mismatch`。

## 扩展适配器

新协议实现 `Upstream` 接口，并通过 `NewChannel(..., mappings...)` 显式注册字段映射即可。通道继续通过同一个 `TraceStore` 记录完整来回；新增适配器不得在写入结果不确定时自行重试。
