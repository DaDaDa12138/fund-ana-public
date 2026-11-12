# 公开标准 Runner 与私有运行存储

## 两个仓库的职责

| 仓库 | 可见性 | 内容 |
|---|---|---|
| `DaDaDa12138/fund-ana-public` | 公开 | 审核后的源码、模板、文档及 GitHub 托管执行工作流 |
| `DaDaDa12138/fund-ana` | 私有 | `cloud-state` 索引、私有 Release 对象、`daily-reports` 正式报告及执行收据、私有诊断日志 |

旧仓库保持私有，不改变其历史分支、Release、原manifest或数据内容。公开仓库从审核后的源码快照建立独立历史，不能镜像推送旧仓库的所有refs。公开仓库不得保存任何生产数据库、冻结输入、原始API响应或完整正式报告。

公开仓库使用标准 `ubuntu-24.04` 托管Runner；按GitHub公共仓库规则，其标准Runner计算免费。大型Runner及存储等另有规则；本次不启用付费预算，也不把免费计算解释为无限资源或准点调度。

## 凭据与信任边界

- 在公开执行仓库的 Actions Secrets 设置 `FUND_ANA_STATE_TOKEN`。建议使用fine-grained PAT，仅选择私有仓库 `DaDaDa12138/fund-ana`，Repository Contents为Read and write，Metadata为自动只读；不需要Actions/Administration权限。设定合理有效期并在到期前轮换。
- 不把本机既有宽权限登录令牌复制为生产secret；令牌不得进入源码、配置、日志、收据或聊天。
- 生产job的默认 `GITHUB_TOKEN` 仅有contents:read/actions:read，用于源码和本次运行时序；私有存储令牌仅注入需要访问私有仓库的host步骤，不传入业务/web/代理容器。
- `FUND_ANA_STATE_REPOSITORY`明确绑定私有存储仓库；恢复前校验其身份和可见性，报告同步再次拒绝公开目标。凭据缺失、权限失效、仓库公开或归档时失败，不从空历史继续。
- 生产入口只接受受信任仓库main的schedule/workflow_dispatch。源码pull_request测试不注入生产secret，无pull_request_target执行通道。运行生产还须仓库变量 `FUND_ANA_PRODUCTION_ENABLED=true`。
- 标准Runner是临时环境，会暂时解密并处理私有业务数据。源码、运行步骤及非敏感状态可公开；详细业务输出转入私有诊断日志。不能把“私有持久化”误称为第三方Runner永远不接触数据。

## 调度与日更完成证明

时区统一 `Asia/Shanghai`。GitHub服务端cron默认UTC：

| UTC cron | 上海执行意图 |
|---|---|
| `47 23 * * *` | 次日07:47主日更 |
| `17 0,1,4 * * *` | 08:17、09:17、12:17有限补偿 |
| `47 2 * * *` | 10:47补偿 |
| `17 19 * * 6` | 周日03:17数据维护 |

已完成当天07:47之后真实采集、终态、状态持久化及报告同步才跳过昂贵恢复。私有仓库中的旧收据继续作为权威；旧记录没有源码仓库字段时保留旧身份，新记录同时保存 `github_repository` 与 `storage_repository`。同净值日保旧不制造新期数，人工publish_only及只读schedule_check不冒充日更。并发组不取消活动任务，但跨仓库的concurrency互不相通，切换时必须先停旧生产工作流并确认无活动任务。

GitHub明确可能延迟或丢弃schedule事件。有限补偿降低漏跑风险，不保证准点；Runner的TZ不会改变服务端cron时区，公开仓库也不保证更高调度优先级。公开仓库长时间无活动可能停用定时工作流，应持续核验运行收据而非只看绿色状态。

## 单次执行与唯一提交顺序

1. 检出受信任main源码；确认私有存储身份、令牌及受信任事件。缺凭据在任何数据恢复前停止。
2. 从私有 `daily-reports/checks`读取当天完成收据，用源码仓库token读取当前Action创建时间；两者的权限和仓库分开。
3. 完整SHA校验私有 `cloud-state` 索引及Release分块，恢复SQLite/账本、正式历史、冷热库及冻结依赖。磁盘不足或历史准入事实变化时阻断。
4. 从审核模板生成临时配置，精确保留四份运行策略文件；构建镜像并校验源码SHA。API代理位于同一个托管Runner，不调用本机、SSH或自托管Runner。
5. 既有scheduler --once入口运行真实采集、风险计算、多周期筛选、项目门禁、累计跟踪、前瞻验证及冻结回放；不改变金融门槛、公式或NULL语义。
6. Runner内 `.publication_current`仍是唯一正式提交点；发布锁内核验原manifest和产物SHA。
7. 验证health、dashboard/latest、tracker/current、scheduler/status/detail与同一正式身份和数量一致。
8. 停止web写入，原生一致SQLite备份及逐对象SHA校验后，快进更新私有cloud-state；必须替换本次恢复的同一状态head。
9. 状态保存成功后，才同步私有daily-reports。报告同步失败可用publish_only补同步；不能退回旧状态制造相同代际。旧原件、manifest和失败收据不回写。
10. 详细日志仅按允许清单收集，对两种token及Basic编码脱敏后，上传私有 `cloud-action-diagnostics-YYYYMM` Release。公开日志不打印名单、NAV、bundle或失败任务尾部，不使用公开artifact上传业务材料。诊断保存失败保留失败状态，不能伪称日志已经保存。

原degraded不改写；仅已验证的明确空收益、初始化异常或被拒旧响应对应的正式排除，并通过DB证明、SHA核验CSV无交集、计数及原发布门禁后允许自动化带warning完成。旧响应状态还必须重新核验当前排名来源SHA、代码、净值、日期及十项收益，使用`github-safe-publication-completion-v2`绑定证明；基金自身日期与报告基准日期分别保留，必须符合原采集资格日期及最大净值年龄窗口；缺证、正式重叠或额外失败仍阻断。旧v1收据兼容保留。其他API、计算、恢复、持久化及同步失败仍失败。

## 初次切换与回退

1. 旧仓库始终私有。审计公开源码允许清单、凭据及二进制，建立独立公开源码历史；禁止 `git push --mirror`。
2. 配置受限secret，先保持 `FUND_ANA_PRODUCTION_ENABLED=false`。隔离Docker验证权限分离、公开目标拒绝、历史兼容和日志脱敏。
3. 确认旧生产任务无queued/in_progress后，禁用旧私有仓库Daily Fund Reports，记录最后状态/报告提交；旧仓库不再承担Runner计算。
4. 在新仓库设置production变量true，运行schedule_check验证跨仓库私有收据、时序和缺失语义；再验收一次真实日更，状态先于报告保存、四项API和冻结回放通过。
5. 继续验收自然日更及周维护，人工success不替代schedule。出现权限/恢复问题时先关闭新仓库production开关，确认无活动任务，再恢复旧工作流；保留最后实际已提交的私有state，不将其回退。

不要同时启用新旧生产入口。旧仓库的源码main保留作为迁移前恢复证据，新的源码维护入口为公开仓库；不因为存储留有main就让它再次自动执行。

## 命令

```bash
# 人工云端运行。这里是公开源码/执行仓库。
gh workflow run daily-reports.yml --repo DaDaDa12138/fund-ana-public -f operation=schedule_check
gh workflow run daily-reports.yml --repo DaDaDa12138/fund-ana-public -f operation=daily_refresh
gh workflow run daily-reports.yml --repo DaDaDa12138/fund-ana-public -f operation=publish_only

# 运维恢复/状态查询。认证需仅能访问私有存储的令牌。
python3 scripts/manage_github_cloud_state.py status --repository DaDaDa12138/fund-ana --receipt /tmp/cloud-status.json
python3 scripts/manage_github_cloud_state.py restore --repository DaDaDa12138/fund-ana --receipt .cloud-restored.json
```

手工恢复必须在无运行库的新工作目录执行；不把生产数据或配置挂进单元测试。正式历史原件、独特材料和冻结输入继续沿用已有生命周期保护，云端对象通用永久回收保持关闭，不能按Release日期删除。

## 切换与维护状态

公开源码仓库已启用，原私有仓库的三份旧工作流已停用，生产变量在公开仓库开启。跨仓库鉴权及今日私有收据读取已通过只读检查；真实日更的完整验收以私有执行收据和正式文件SHA为准。

本地网页可独立部署，但不承担云端日更。新的源码维护入口是公开仓库，旧私有main仅保留迁移前恢复证据；不要将公开执行工作流再推送到旧main并重新启用。

公开源码的相关权限、时序、状态、报告及完成证明测试已在断网Docker中通过。新克隆须先用源码模板创建本地配置；真实基金回归样本不在公开仓库，完整样本回归应在获授权的私有环境执行。自然日更和周维护仍须单独观察，不能以人工运行代替。

## 访问与故障处理

- [公开源码与Actions](https://github.com/DaDaDa12138/fund-ana-public)；[私有正式报告](https://github.com/DaDaDa12138/fund-ana/tree/daily-reports)，后者仅授权用户可访问。
- `private_storage_token_missing`：配置secret；`private_storage_authentication_failed`：检查令牌有效期、只选定的仓库和Contents权限；不要把401/403当成没有历史。
- `private_storage_identity_invalid` / `reports_require_private_repository`：检查目标，保持数据仓库私有，不删除保护检查。
- `cloud_history_decisions_changed_after_restore`：核对历史血缘/原件SHA，不能删除绑定或修改旧事实消除阻断。
- `cloud_state_changed_since_restore`：检查是否有另一生产写入，按最后已提交状态恢复；不要强推覆盖。
- `cloud_runner_disk_insufficient`：按真实空间和恢复容量评估，不能删除生产历史凑空间。
- 上游缺失保持NULL，实时估值可选；失败保留旧报告，Action绿色不证明所有供应商字段完整。

依据：[Actions计费](https://docs.github.com/zh/billing/concepts/product-billing/github-actions)、[定时事件](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule)。本文描述运行契约，不公开实际基金名单、NAV或私有审计收据。
