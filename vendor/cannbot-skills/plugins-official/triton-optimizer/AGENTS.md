# Triton Optimizer Workflows

本插件是 Triton Optimizer 在 CANNBot 中的 workflow 入口，覆盖两类场景：

- `triton-agent-optimize`：优化已有 Triton Ascend NPU 算子，按 baseline/round 方式迭代。
- `triton-agent-convert`：把 PyTorch 算子转换为 PyTorch-facing、Triton Ascend NPU-backed 的实现，并完成验证。

## 入口职责

- 优化任务优先使用 `agents/triton-agent-optimize.md` 中的 `triton-agent-optimize` agent。
- 转换任务优先使用 `agents/triton-agent-convert.md` 中的 `triton-agent-convert` agent。
- 若当前工具不支持 Subagent，则按本文件规则在当前会话中直接执行 workflow。
- 所有 Skills 来自仓库 `ops/` 目录，通过安装脚本或插件依赖注入；插件目录不保留重复 skill 副本。
- `.triton-agent/` 是优化 workflow 运行态目录，由 hooks 与状态 skill 管理；不要手动编辑或删除。

## 可用 Skills

| Skill | 用途 |
| --- | --- |
| `triton-npu-optimize` | 主优化 workflow |
| `triton-npu-optimize-knowledge` | Triton NPU 优化模式、症状与经验知识 |
| `triton-npu-convert-pytorch-operator` | PyTorch 算子到 Triton Ascend NPU 实现的转换 workflow |
| `ascend-npu-prepare-optimize-baseline` | 建立或修复 baseline |
| `ascend-npu-gen-test` | 生成正确性测试 |
| `ascend-npu-gen-bench` | 生成 benchmark |
| `ascend-npu-run-eval` | 执行 test/bench/profile |
| `ascend-npu-optimize-state` | 管理 baseline 与 round 状态 |
| `ascend-npu-profile-operator` | 解析 NPU profiling 结果 |
| `ascend-npu-analyze-round-performance` | 分析单轮优化收益与瓶颈 |
| `triton-npu-analyze-ir` | 采集和分析 Triton IR |
| `triton-npu-analyze-compiler-source` | 按性能问题定位编译器源码线索 |
| `triton-npu-repair-guide` | 修复常见失败模式 |

## 优化 Workflow

详细流程以 `agents/triton-agent-optimize.md` 为准，按以下 Phase 执行：

```
Phase 0: 明确 operator、target mode、评测命令和运行环境
Phase 1: 建立或复用 canonical baseline
Phase 2: 创建 opt-round-N，并在首次代码修改前完成方向选择
Phase 3: 每轮只实施一个主要优化点
Phase 4: 依次执行 correctness、benchmark、compare-perf
Phase 5: 必要时按 pattern -> profile -> IR -> compiler-source 升级证据
Phase 6: submit-round、更新 opt-note.md，并决定停止或进入下一轮
```

Phase 2 方向选择不固定为 pattern-index 单次门禁，但也不是自由发挥。每轮首次代码修改前必须完成 workflow context review：读取 Phase 1 已接受的 baseline state、`opt-note.md`、当前/历史 `attempts.md`、目标 operator 的 wrapper/kernel 结构和用户目标；然后从允许的 direction source 中选择一个 concrete hypothesis，并写清 success criteria、验证命令、evidence、命中条件、预期修改代码区域和未选方向原因。方向优先按 `triton-npu-optimize-knowledge/references/pattern_index.md` 的现有结构推进：先检查 `High Priority Patterns`，再检查适用的 `Generated Pattern Summaries` 结构性 pattern，最后才进入 pattern 明确允许的 bounded tuning/cleanup。benchmark/compare-perf、profile、IR、compiler-source 只作为证据升级、问题归因或 pattern 轨道释放后的依据，不作为随意试错入口。若连续 3 轮及以上 kernel 内部优化收益低于 1.2x，或证据显示 wrapper/aclnn/TensorMove/launch overhead 主导，或 benchmark 总耗时与 kernel 耗时明显不匹配，必须暂停 kernel micro-optimization 并回到 pattern-index triage，优先寻找 architecture-level 或 structural pattern。Free exploration 只有在适用的 high-priority、结构性和 bounded tuning/cleanup pattern 都尝试、拒绝或释放后才允许。禁止把多轮微调作为优化方向，不允许连续多轮只调整 `BLOCK_SIZE`、tile size、grid、`num_warps`、launch flag 或阈值常量；pattern/autotune 明确要求的参数搜索必须是有界候选集或 helper workflow。

## 转换 Workflow

1. 明确原始 PyTorch 算子文件、转换输出路径、验证模式和运行环境。
2. 使用 `triton-npu-convert-pytorch-operator` 读取原始算子，但不修改原文件。
3. 将转换后的 PyTorch-facing Triton Ascend NPU 实现写入用户指定输出路径。
4. 保留源文件尾部的输入辅助函数块，便于后续 harness 生成和差分验证。
5. 使用 `ascend-npu-gen-test` 复用或生成测试，再用 `ascend-npu-run-eval` 执行验证。
6. 遇到 Triton 编译、JIT、launch 或 kernel 结构问题时，使用 `triton-npu-repair-guide` 修复后重跑验证。

## 约束

- 优化流程一次只保持一个 active round。
- 不跳过正确性验证直接比较性能。
- 不把测试、benchmark、profile 结果写成口头假设，必须保留可追溯 artifact。
- 转换流程不得覆盖原始 PyTorch 算子文件。
- 转换结果必须真实走 Triton Ascend NPU kernel 路径，不能用 PyTorch 计算路径伪装替代。
- 不把 `ops/` 下的 Skill 内容复制回插件目录；插件只保留 agent、hooks 和安装入口。
