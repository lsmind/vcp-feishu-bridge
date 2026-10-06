# VCPFeishuBridge

飞书 ⇄ [VCPToolBox](https://github.com/lioensky/VCPToolBox) 双向桥接插件。让 VCP 生态的 Agent（炼丹师、小影等）常驻飞书群聊：收发消息、渲染富文本、写日记、刷新 RAG 记忆，并支持**工具调用审批在群内一键闭环**。

> 从 VCPToolBox v34.6 拆出独立维护，与上游隔离演进。

## 功能

### 消息桥接
- WebSocket 长连接（无需公网回调/内网穿透），多群多话题路由
- 消息流式回复（先提示语后正文）、用户白名单
- 话题会话复用（feishu topic ⇄ VCPChat session）

### 富文本渲染链（对齐 VCPChat 桌面端效果）
- 工具调用摘要折叠卡
- Mermaid 流程图/时序图离线渲染为图片投递（Puppeteer）
- 代码块语法高亮、Markdown 表格、LaTeX 公式
- 协议标记剥离、防截断处理

### 工具审批群内闭环（v34.x）
Agent 要执行危险命令（非白名单 Shell 等）时，审批卡自动发到指定群：

```
Agent 请求危险工具 → 群内橙色待审卡 🔔
  点 ✅ 批准 → 卡片变绿 → vcpApproved 标记透传 → 命令执行
  点 ❌ 拒绝 → 卡片变红 → 命令不执行
  5 分钟超时 → 卡片置灰「已失效」
  重复点击  → decision 落盘恢复正确终态（进程重启不丢）
```

架构要点：进程事件总线（`vcp:approval-request` / `vcp:approval-expired`）零循环依赖；WS 模式下事件回调返回值会被 SDK 丢弃，卡片终态更新必须主动 `PATCH /im/v1/messages/:id`；审批通过的调用带 `vcpApproved` 标记，插件安全分级据此放行（黑名单/高危仍拦截）。

## 目录

```
VCPFeishu/          炼丹师桥：全功能（富文本 + 审批闭环 + 日记 + RAG）
VCPFeishuXiaoying/  小影桥：话题会话桥接
scripts/install.sh  部署脚本
```

## 安装

前置：可运行的 [VCPToolBox](https://github.com/lioensky/VCPToolBox) 实例（需包含 `POST /v1/tool-approval` 路由与 `vcp:approval-request` 事件广播，v34 起）。

```bash
git clone https://github.com/lsmind/vcp-feishu-bridge.git
cd vcp-feishu-bridge
./scripts/install.sh /path/to/VCPToolBox
```

脚本会：备份已存在的旧插件 → 复制插件文件 → 保留你已有的 `config.env`（凭证不覆盖）。

### 配置

```bash
cd /path/to/VCPToolBox/Plugin/VCPFeishu
cp config.env.example config.env
```

必填：

| 键 | 说明 |
|---|---|
| `FeishuAppId` / `FeishuAppSecret` | 飞书自建应用凭证 |
| `FeishuBindAgent` | 绑定的 VCPChat Agent（目录名/id/name） |

审批功能（可选）：

| 键 | 说明 |
|---|---|
| `FeishuApprovalTargetId` | 审批卡投递群 `chat_id`（`oc_` 开头）；留空则只推管理面板 |
| `VCPServerPort` | VCP 主服务 HTTP 端口（默认 6005），审批按钮回调目标 |

飞书应用需要的权限：消息收发（WS 模式）、图片上传。卡片按钮回调走 WS 事件推送，**无需单独开通卡片交互权限**。

> 注意：`config.env` 新增键必须同步在 `plugin-manifest.json` 的 `configSchema` 里声明，否则会被配置加载层静默过滤。

### 启动

随 VCPToolBox 主服务启动（pm2 / systemd 均可）：

```bash
pm2 restart vcptoolbox
```

## 运维速查

- 审批状态是内存态：5 分钟 TTL，进程重启即作废（历史 decision 落盘于插件目录 `approvalDecisions.json`，保留最近 200 条）
- 卡片 PATCH 后若客户端仍显示旧状态，下拉刷新即可（飞书客户端渲染缓存）
- 按钮事件键名为 `card.action.trigger`（无 ed），代码已双注册兜底

## 路线

- [x] v32 三件套（富文本/折叠卡/图片）
- [x] v33 Mermaid 离线渲染
- [x] v34 审批飞书群内闭环（拦截→发卡→按钮→PATCH 终态→透传放行→decision 落盘）
- [ ] 小影插件审批卡独立配置

## License

MIT
