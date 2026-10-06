#!/usr/bin/env bash
# VCPFeishu Bridge 安装脚本: 复制插件到 VCPToolBox 的 Plugin/ 目录
# 用法: ./scripts/install.sh /path/to/VCPToolBox
set -euo pipefail

VCP_DIR="${1:?用法: $0 /path/to/VCPToolBox}"
[ -d "$VCP_DIR/Plugin" ] || { echo "错误: $VCP_DIR/Plugin 不存在, 请确认 VCPToolBox 路径"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BRIDGE_DIR="$(dirname "$SCRIPT_DIR")"

install_plugin() {
    local name="$1"
    local dst="$VCP_DIR/Plugin/$name"
    if [ -d "$dst" ]; then
        local bak="$dst.bak-$(date +%Y%m%d-%H%M%S)"
        echo "[备份] $dst -> $bak"
        cp -r "$dst" "$bak"
    fi
    mkdir -p "$dst"
    # 只装活文件, 不装运行时数据(approvalDecisions.json)与凭证(config.env)
    for f in feishuBot.js index.js plugin-manifest.json config.env.example README.md; do
        [ -f "$BRIDGE_DIR/$name/$f" ] && cp "$BRIDGE_DIR/$name/$f" "$dst/"
    done
    # 保留已有的 config.env(凭证)
    if [ -f "$dst/config.env" ]; then
        echo "[保留] $dst/config.env 已存在, 未覆盖(凭证与目标群配置)"
    else
        echo "[提示] $dst/config.env 不存在, 请从 config.env.example 创建并填入凭证"
    fi
    echo "[完成] $name 已安装到 $dst"
}

install_plugin VCPFeishu
install_plugin VCPFeishuXiaoying

cat <<'EOF'

后续步骤:
1. 编辑两个插件的 config.env(参考 config.env.example)
2. 主 VCPFeishu 需要的审批配置:
   FeishuApprovalTargetId=<审批卡发送的群 chat_id>
   VCPServerPort=6005
3. 重启 VCPToolBox(pm2 restart vcptoolbox)
EOF
