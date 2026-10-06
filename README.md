# VCPFeishu Bridge

飞书 ↔ VCPToolBox 桥接插件（独立维护版，与上游 [VCPToolBox](https://github.com/lioensky/VCPToolBox) 隔离）。

包含两个插件：

- **VCPFeishu**（炼丹师）：飞书 WebSocket 长连接桥，富文本渲染（折叠卡/Mermaid 图/代码块/表格）、日记、RAG 记忆刷新、**工具调用审批飞书群内闭环**（v34.x）
- **VCPFeishuXiaoying**（小影）：话题会话桥接，复用 VCP 后端生成回复

## 审批闭环架构（v34.x）

```
危险工具调用 → 审批判定 → 群内橙色待审卡
  → 点 ✅批准 → 卡片变绿 → vcpApproved 标记透传 → 命令执行
  → 点 ❌拒绝 → 卡片变红 → 命令不执行
  → 5min 超时 → 卡片置灰"已失效"
  → 重复点击 → decision 落盘恢复正确终态（进程重启不丢）
```

关键实现：进程事件总线（`vcp:approval-request/expired`）零循环依赖；WS 模式下回调返回值被 SDK 丢弃，卡片终态更新须主动 `PATCH /im/v1/messages/:id`；message_id 在事件 `data.context.open_message_id`。

## 安装

```bash
./scripts/install.sh /path/to/VCPToolBox
```

将两个插件复制进 VCPToolBox 的 `Plugin/` 目录（已存在则备份后覆盖），凭证配置见各插件 `config.env.example`。审批功能需在 `plugin-manifest.json` configSchema 声明的键（`FeishuApprovalTargetId`/`VCPServerPort`）写入 `config.env`。

## 依赖

- VCPToolBox 主服务（`server.js` 需含 `POST /v1/tool-approval` 路由、`Plugin.js` 需含 `vcp:approval-request` 事件广播——v34 起）

## 路线

- [x] v32 三件套（富文本/折叠卡/图片）
- [x] v33 Mermaid 渲染
- [x] v34 审批飞书群内闭环（拦截→发卡→按钮→PATCH 终态→透传放行→decision 落盘）
- [ ] 小影插件审批卡独立配置
