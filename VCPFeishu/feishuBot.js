'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PLUGIN_NAME = 'VCPFeishu';
const MESSAGE_DEDUPE_TTL_MS = 10 * 60 * 1000;
const FEISHU_HTTP_TIMEOUT_MS = 15000;
// VCP Loop（多轮工具调用）耗时远超普通 HTTP 请求，用专用 undici dispatcher
// 覆盖 undici 默认 headersTimeout=300s —— 否则 5 分钟后 fetch 以 "fetch failed" 假死
const VCP_LOOP_DISPATCHER = require('undici').Agent
    ? new (require('undici').Agent)({ headersTimeout: 1800_000, bodyTimeout: 1800_000 })
    : undefined;
const FEISHU_WS_READY_TIMEOUT_MS = 30000;
const DEFAULT_STREAM_HINT = '正在思考中…';

let lark = null;
let wsClient = null;
let config = {};
let debugMode = false;
let tenantTokenCache = { appId: '', appSecret: '', token: '', expiresAt: 0 };
const processedMessageIds = new Map();

const stats = {
    connected: false,
    messagesReceived: 0,
    messagesProcessed: 0,
    messagesFailed: 0,
    topicsCreated: 0,
    lastMessageAt: null,
    lastError: null,
    startedAt: null,
    lastTopicId: null,
    lastSessionKey: null,
};

function log(...args) { console.log(`[${PLUGIN_NAME}][Bot]`, ...args); }
function warn(...args) { console.warn(`[${PLUGIN_NAME}][Bot]`, ...args); }
function debug(...args) { if (debugMode) console.log(`[${PLUGIN_NAME}][Bot][debug]`, ...args); }

function configure(pluginConfig = {}) {
    config = { ...config, ...pluginConfig };
    debugMode = toBoolean(getConfigValue('DebugMode'), false);
}

function getConfigValue(...keys) {
    for (const key of keys) {
        if (config[key] !== undefined && config[key] !== null && config[key] !== '') return config[key];
        if (process.env[key] !== undefined && process.env[key] !== null && process.env[key] !== '') return process.env[key];
    }
    return '';
}

function toBoolean(value, fallback = false) {
    if (value === undefined || value === null || value === '') return fallback;
    if (typeof value === 'boolean') return value;
    return ['true', '1', 'yes', 'on'].includes(String(value).trim().toLowerCase());
}

function splitList(value) {
    if (!value) return [];
    if (Array.isArray(value)) return value.map(String).map(v => v.trim()).filter(Boolean);
    return String(value).split(',').map(v => v.trim()).filter(Boolean);
}

function setLastError(err) {
    stats.lastError = {
        message: String(err?.message || err || ''),
        at: new Date().toISOString(),
    };
}

function projectRoot() {
    const basePath = config.PROJECT_BASE_PATH || process.env.PROJECT_BASE_PATH || path.resolve(__dirname, '..', '..', '..');
    return path.basename(basePath).toLowerCase() === 'vcpdistributedserver'
        ? path.dirname(basePath)
        : basePath;
}

function appDataRoot() {
    return path.join(projectRoot(), 'AppData');
}

function settingsPath() {
    return path.join(appDataRoot(), 'settings.json');
}

function agentsDir() {
    return path.join(appDataRoot(), 'Agents');
}

function userDataDir() {
    return path.join(appDataRoot(), 'UserData');
}

function readJson(filePath, fallback = null) {
    try {
        if (!fs.existsSync(filePath)) return fallback;
        return JSON.parse(fs.readFileSync(filePath, 'utf8'));
    } catch (err) {
        warn(`读取 JSON 失败: ${filePath} - ${err.message}`);
        return fallback;
    }
}

function writeJson(filePath, value) {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, JSON.stringify(value, null, 2), 'utf8');
}

function loadSettings() {
    return readJson(settingsPath(), {}) || {};
}

function loadBridgeConfig() {
    return {
        appId: String(getConfigValue('FeishuAppId') || '').trim(),
        appSecret: String(getConfigValue('FeishuAppSecret') || '').trim(),
        bindAgent: String(getConfigValue('FeishuBindAgent') || '').trim(),
        allowedUsers: splitList(getConfigValue('FeishuAllowedUsers')),
        streamReply: toBoolean(getConfigValue('FeishuStreamReply'), true),
        inflightTimeoutMs: Number.parseInt(getConfigValue('FeishuInflightTimeoutMs'), 10) || 120000,
        streamHint: String(getConfigValue('FeishuStreamHint') || DEFAULT_STREAM_HINT),
        approvalTargetId: String(getConfigValue('FeishuApprovalTargetId') || '').trim() || null,
        serverPort: Number.parseInt(getConfigValue('VCPServerPort') || getConfigValue('FeishuServerPort'), 10) || 6005,
    };
}

function randomSuffix(length = 7) {
    return crypto.randomBytes(Math.ceil(length / 2)).toString('hex').slice(0, length);
}

function safeIdPart(value) {
    return String(value || 'unknown')
        .trim()
        .replace(/[^\w.-]+/g, '_')
        .replace(/^_+|_+$/g, '')
        .slice(0, 80) || 'unknown';
}

function sessionKeyFor(chatId, senderId) {
    return `feishu_${safeIdPart(chatId || senderId || 'chat')}`;
}

function normalizeHistory(value) {
    return Array.isArray(value) ? value : [];
}

function getAgentDisplayName(agentId, agentConfig) {
    return agentConfig?.name || agentConfig?.chineseName || agentConfig?.baseName || agentId;
}

function findAgent(bindAgent) {
    const wanted = String(bindAgent || '').trim();
    const wantedLower = wanted.toLowerCase();
    if (!wanted || !fs.existsSync(agentsDir())) return null;

    for (const folder of fs.readdirSync(agentsDir())) {
        const folderPath = path.join(agentsDir(), folder);
        if (!fs.statSync(folderPath).isDirectory()) continue;
        const configPath = path.join(folderPath, 'config.json');
        const agentConfig = readJson(configPath, null);
        if (!agentConfig) continue;

        const aliases = [
            folder,
            agentConfig.id,
            agentConfig.name,
            agentConfig.chineseName,
            agentConfig.baseName,
        ].map(v => String(v || '').trim()).filter(Boolean);

        if (aliases.includes(wanted) || aliases.some(alias => alias.toLowerCase() === wantedLower)) {
            return { id: folder, dir: folderPath, configPath, config: agentConfig };
        }
    }
    return null;
}

function listFeishuTopics(bindAgent = '') {
    const bridgeConfig = loadBridgeConfig();
    const agent = findAgent(bindAgent || bridgeConfig.bindAgent);
    if (!agent) throw new Error(`未找到绑定 Agent: ${bindAgent || bridgeConfig.bindAgent}`);

    const latestConfig = readJson(agent.configPath, agent.config) || {};
    const topics = Array.isArray(latestConfig.topics) ? latestConfig.topics : [];

    return topics
        .filter(topic => String(topic?._metadata?.source || '') === PLUGIN_NAME)
        .map(topic => {
            const meta = topic._metadata || {};
            return {
                topicId: topic.id || null,
                name: topic.name || null,
                createdAt: topic.createdAt || null,
                session: meta.sessionKey || null,
                target: meta.targetId || meta.chatId || meta.userId || null,
                receiveIdType: meta.receiveIdType || null,
                chatType: meta.chatType || null,
                chatId: meta.chatId || null,
                userId: meta.userId || null,
            };
        });
}

function listFeishuGroups(bindAgent = '') {
    return listFeishuTopics(bindAgent).filter(item => item.chatType === 'group');
}

async function fetchFeishuChatInfo(chatId, bridgeConfig = loadBridgeConfig()) {
    if (!chatId) return null;
    const token = await tenantAccessToken(bridgeConfig);
    const response = await fetchWithTimeout(
        `https://open.feishu.cn/open-apis/im/v1/chats/${encodeURIComponent(chatId)}`,
        {
            method: 'GET',
            headers: { Authorization: `Bearer ${token}` },
        },
        FEISHU_HTTP_TIMEOUT_MS,
        '获取飞书群聊信息'
    );
    const text = await response.text();
    const data = text ? JSON.parse(text) : {};
    if (!response.ok) throw new Error(data.msg || data.message || `HTTP ${response.status}`);
    assertFeishuOk(data, '获取飞书群聊信息');
    return data.data?.chat || null;
}

async function fetchFeishuUserInfo(userId, bridgeConfig = loadBridgeConfig()) {
    if (!userId) return null;
    const token = await tenantAccessToken(bridgeConfig);
    const response = await fetchWithTimeout(
        `https://open.feishu.cn/open-apis/contact/v3/users/${encodeURIComponent(userId)}?user_id_type=open_id`,
        {
            method: 'GET',
            headers: { Authorization: `Bearer ${token}` },
        },
        FEISHU_HTTP_TIMEOUT_MS,
        '获取飞书用户信息'
    );
    const text = await response.text();
    const data = text ? JSON.parse(text) : {};
    if (!response.ok) throw new Error(data.msg || data.message || `HTTP ${response.status}`);
    assertFeishuOk(data, '获取飞书用户信息');
    return data.data?.user || null;
}

async function enrichFeishuTopic(item, bridgeConfig = loadBridgeConfig()) {
    const enriched = { ...item, displayName: item.name || item.target || item.topicId };
    try {
        if (item.chatType === 'group' && item.chatId) {
            const chat = await fetchFeishuChatInfo(item.chatId, bridgeConfig);
            if (chat) {
                enriched.displayName = chat.name || enriched.displayName;
                enriched.chatName = chat.name || null;
                enriched.memberCount = chat.member_count ?? null;
                enriched.description = chat.description || null;
            }
            return enriched;
        }

        if (item.userId) {
            const user = await fetchFeishuUserInfo(item.userId, bridgeConfig);
            if (user) {
                enriched.displayName = user.name || user.en_name || enriched.displayName;
                enriched.userName = user.name || null;
                enriched.userEnName = user.en_name || null;
            }
        }
    } catch (err) {
        enriched.lookupError = err.message;
    }
    return enriched;
}

async function listFeishuTopicsDetailed(bindAgent = '') {
    const bridgeConfig = loadBridgeConfig();
    const topics = listFeishuTopics(bindAgent);
    return Promise.all(topics.map(item => enrichFeishuTopic(item, bridgeConfig)));
}

async function listFeishuGroupsDetailed(bindAgent = '') {
    const groups = listFeishuGroups(bindAgent);
    const bridgeConfig = loadBridgeConfig();
    return Promise.all(groups.map(item => enrichFeishuTopic(item, bridgeConfig)));
}

function inferReceiveIdType(target, explicitType) {
    const explicit = String(explicitType || '').trim();
    if (explicit) return explicit;
    const id = String(target || '').trim();
    if (id.startsWith('ou_')) return 'open_id';
    if (id.startsWith('oc_')) return 'chat_id';
    return 'chat_id';
}

function topicTitleFor(chatType, targetId) {
    const suffix = safeIdPart(targetId).slice(0, 32);
    return `飞书${chatType === 'group' ? '群聊' : '私聊'} ${suffix}`;
}

function topicHistoryPath(agentId, topicId) {
    return path.join(userDataDir(), agentId, 'topics', topicId, 'history.json');
}

function findFeishuTopic(agentConfig, matchValue) {
    const topics = Array.isArray(agentConfig?.topics) ? agentConfig.topics : [];
    if (!matchValue) return null;
    return topics.find(topic => {
        const meta = topic?._metadata || {};
        return topic.id === matchValue
            || meta.sessionKey === matchValue
            || meta.chatId === matchValue
            || meta.userId === matchValue;
    }) || null;
}

function ensureFeishuTopic(agent, session) {
    const latestConfig = readJson(agent.configPath, agent.config) || {};
    latestConfig.topics = Array.isArray(latestConfig.topics) ? latestConfig.topics : [];

    let topic = findFeishuTopic(latestConfig, session.sessionKey);
    let created = false;
    if (!topic) {
        const now = Date.now();
        topic = {
            id: `topic_${now}`,
            name: topicTitleFor(session.chatType, session.targetId),
            createdAt: now,
            locked: false,
            unread: true,
            creatorSource: `plugin:${PLUGIN_NAME}`,
            _metadata: {
                source: PLUGIN_NAME,
                sessionKey: session.sessionKey,
                chatId: session.chatId || null,
                userId: session.senderId || null,
                targetId: session.targetId || null,
                receiveIdType: session.receiveIdType,
                chatType: session.chatType,
            },
        };
        latestConfig.topics.unshift(topic);
        created = true;
        stats.topicsCreated++;
        log(`首次连接已创建话题会话: agent=${agent.id} topic=${topic.id} session=${session.sessionKey}`);
    } else {
        topic.unread = true;
        topic.locked = false;
        topic._metadata = {
            ...(topic._metadata || {}),
            source: PLUGIN_NAME,
            sessionKey: session.sessionKey,
            chatId: session.chatId || topic._metadata?.chatId || null,
            userId: session.senderId || topic._metadata?.userId || null,
            targetId: session.targetId || topic._metadata?.targetId || null,
            receiveIdType: session.receiveIdType || topic._metadata?.receiveIdType || null,
            chatType: session.chatType || topic._metadata?.chatType || null,
        };
    }

    writeJson(agent.configPath, latestConfig);

    const historyPath = topicHistoryPath(agent.id, topic.id);
    if (!fs.existsSync(historyPath)) writeJson(historyPath, []);

    stats.lastTopicId = topic.id;
    stats.lastSessionKey = session.sessionKey;
    return { agentId: agent.id, agentConfig: latestConfig, topic, historyPath, created, session };
}

function buildUserMessage(text, session, timestamp = Date.now()) {
    return {
        role: 'user',
        name: session.senderName || '飞书用户',
        content: text,
        timestamp,
        id: `msg_${timestamp}_user_${randomSuffix()}`,
        attachments: [],
        _metadata: {
            source: PLUGIN_NAME,
            feishuMessageId: session.messageId || null,
            feishuSenderId: session.senderId || null,
            feishuChatId: session.chatId || null,
            feishuChatType: session.chatType || null,
        },
    };
}

function buildAssistantMessage(text, agentId, agentConfig, session, timestamp = Date.now()) {
    return {
        role: 'assistant',
        name: getAgentDisplayName(agentId, agentConfig),
        content: text,
        timestamp,
        id: `msg_${timestamp}_assistant_${randomSuffix()}`,
        isThinking: false,
        avatarUrl: agentConfig.avatarUrl || 'assets/default_avatar.png',
        avatarColor: agentConfig.avatarCalculatedColor || agentConfig.avatarColor || 'rgb(96,106,116)',
        isGroupMessage: false,
        agentId,
        finishReason: 'completed',
        _metadata: {
            source: PLUGIN_NAME,
            feishuSourceMessageId: session.messageId || null,
            feishuChatId: session.chatId || null,
            feishuChatType: session.chatType || null,
        },
    };
}

function appendHistory(historyPath, ...messages) {
    const history = normalizeHistory(readJson(historyPath, []));
    history.push(...messages.filter(Boolean));
    writeJson(historyPath, history);
    return history;
}

function stripMentionPrefix(text) {
    const trimmed = String(text || '').trim();
    const match = trimmed.match(/^@\S+\s+([\s\S]+)$/);
    return match && match[1].trim() ? match[1].trim() : trimmed;
}

function historyToVcpMessage(message) {
    if (!message || !['user', 'assistant'].includes(message.role)) return null;
    const content = typeof message.content === 'string' ? message.content : JSON.stringify(message.content || '');
    if (!content) return null;
    const vcpMessage = { role: message.role, content };
    if (message.name) {
        vcpMessage.name = String(message.name).replace(/[^\w-]/g, '_').slice(0, 64) || undefined;
    }
    if (message.id && typeof message.timestamp === 'number') {
        vcpMessage.__vcpchatTimestampMeta = {
            messageId: message.id,
            role: message.role,
            timestamp: message.timestamp,
        };
    }
    return vcpMessage;
}

function sha256(value) {
    return `sha256:${crypto.createHash('sha256').update(value, 'utf8').digest('hex')}`;
}

function messageTextForHash(message) {
    if (typeof message.content === 'string') return message.content;
    if (Array.isArray(message.content)) {
        return message.content.filter(part => part?.type === 'text').map(part => part.text).join('\n');
    }
    return JSON.stringify(message.content || '');
}

function buildVcpChatExtensions(messages) {
    const messageTimestampBindings = [];
    messages.forEach((message, index) => {
        const meta = message.__vcpchatTimestampMeta;
        if (!meta || !meta.messageId || typeof meta.timestamp !== 'number') return;
        messageTimestampBindings.push({
            messageId: meta.messageId,
            role: message.role || meta.role,
            timestamp: meta.timestamp,
            timestampIso: new Date(meta.timestamp).toISOString(),
            source: 'client_history',
            sentMessageHash: sha256(messageTextForHash(message)),
            sentMessageIndex: index,
        });
    });
    if (messageTimestampBindings.length === 0) return null;
    return { schemaVersion: 1, messageMetadataMode: 'hash_only', messageTimestampBindings };
}

function stripInternalMessageFields(messages) {
    return messages.map(message => {
        const { __vcpchatTimestampMeta, ...clean } = message;
        return clean;
    });
}

function formatTopicTime(timestamp) {
    const date = new Date(timestamp);
    return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')} ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
}

function buildSystemPrompt(agentId, agentConfig, topicSession) {
    let prompt = agentConfig.systemPrompt || `你是 ${getAgentDisplayName(agentId, agentConfig)}。`;
    const agentName = getAgentDisplayName(agentId, agentConfig);
    prompt = prompt.replace(/\{\{AgentName\}\}/g, agentName).replace(/\{\{MaidName\}\}/g, agentName);

    const lines = [];
    lines.push(`当前聊天记录文件路径: ${topicSession.historyPath}`);
    if (topicSession.topic?.createdAt) lines.push(`当前话题创建于 ${formatTopicTime(topicSession.topic.createdAt)}`);
    lines.push(`当前飞书会话 session: ${topicSession.session.sessionKey}`);
    lines.push(`当前飞书话题 topic_id: ${topicSession.topic.id}`);
    lines.push(`当前飞书目标 target: ${topicSession.session.targetId}`);
    lines.push(`当前飞书目标类型 receive_id_type: ${topicSession.session.receiveIdType}`);
    lines.push('如需主动给当前飞书会话或其他飞书用户/群发消息，调用 VCPFeishu 的 FeishuSend。');

    const withContext = `${lines.join('\n')}\n\n${prompt}`.trim();
    return withContext.includes('{{VCPFeishu}}') ? withContext : `${withContext}\n\n{{VCPFeishu}}`;
}

function modelConfigFromAgent(agentConfig) {
    const modelConfig = {};
    if (agentConfig.model || agentConfig.modelId) modelConfig.model = agentConfig.model || agentConfig.modelId;
    if (agentConfig.temperature !== undefined && agentConfig.temperature !== null) modelConfig.temperature = Number(agentConfig.temperature);
    if (agentConfig.maxOutputTokens !== undefined && agentConfig.maxOutputTokens !== null) modelConfig.max_tokens = Number.parseInt(agentConfig.maxOutputTokens, 10);
    if (agentConfig.contextTokenLimit !== undefined && agentConfig.contextTokenLimit !== null) modelConfig.contextTokenLimit = Number.parseInt(agentConfig.contextTokenLimit, 10);
    if (agentConfig.top_p !== undefined && agentConfig.top_p !== null) modelConfig.top_p = Number(agentConfig.top_p);
    if (agentConfig.top_k !== undefined && agentConfig.top_k !== null) modelConfig.top_k = Number.parseInt(agentConfig.top_k, 10);
    modelConfig.stream = toBoolean(agentConfig.streamOutput, false);
    return modelConfig;
}

function buildMessagesForVcp(topicSession) {
    const history = normalizeHistory(readJson(topicSession.historyPath, []));
    let messages = history.map(historyToVcpMessage).filter(Boolean);
    const systemPrompt = buildSystemPrompt(topicSession.agentId, topicSession.agentConfig, topicSession);
    // ---- v16: 上下文预算裁剪(全量回流曾把单 topic 撑到 958K 字符, 迟早撞上下文窗) ----
    // 优先级: FeishuContextBudgetTokens 插件配置 > agent contextTokenLimit > 默认 48000; 设 0 关闭。
    // 被裁掉的旧历史不注入任何替身消息——系统提示已含历史文件路径, agent 需要时可自行 ReadFile 全文。
    const rawBudget = Number.parseInt(getConfigValue('FeishuContextBudgetTokens'), 10);
    const agentLimit = Number.parseInt(topicSession.agentConfig && topicSession.agentConfig.contextTokenLimit, 10);
    const budget = Number.isFinite(rawBudget) ? rawBudget : (Number.isFinite(agentLimit) ? agentLimit : 48000);
    if (budget > 0 && messages.length > 6) {
        const reserve = Math.max(4096, Math.ceil((Number.parseInt(topicSession.agentConfig && topicSession.agentConfig.maxOutputTokens, 10) || 8192) / 2));
        const sysTokens = Math.ceil(systemPrompt.length / 1.5);
        const kept = trimMessagesToBudget(messages, budget - sysTokens, reserve, 6);
        if (kept.length < messages.length) {
            log('v16 上下文裁剪: ' + messages.length + '→' + kept.length + ' 条 (预算 ' + budget + ' tok, 系统提示≈' + sysTokens + ')');
            messages = kept;
        }
    }
    messages.unshift({
        role: 'system',
        content: systemPrompt,
    });
    return messages;
}

function vcpUrlFromSettings(settings) {
    const baseUrl = settings.vcpServerUrl;
    if (!baseUrl) throw new Error('settings.json 缺少 vcpServerUrl');
    if (settings.enableVcpToolInjection !== true) return baseUrl;
    try {
        const url = new URL(baseUrl);
        url.pathname = '/v1/chatvcp/completions';
        return url.toString();
    } catch (_) {
        return baseUrl;
    }
}

async function fetchWithTimeout(url, options, timeoutMs = FEISHU_HTTP_TIMEOUT_MS, operationName = 'HTTP 请求') {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    try {
        return await fetch(url, { ...options, signal: controller.signal });
    } catch (err) {
        if (err?.name === 'AbortError') throw new Error(`${operationName} 超时（${timeoutMs}ms）`);
        throw err;
    } finally {
        clearTimeout(timeoutId);
    }
}

async function postJson(url, body, { headers = {}, timeoutMs = FEISHU_HTTP_TIMEOUT_MS, operationName = 'HTTP 请求' } = {}) {
    const response = await fetchWithTimeout(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...headers },
        body: JSON.stringify(body),
    }, timeoutMs, operationName);
    const text = await response.text();
    let data = {};
    if (text) {
        try { data = JSON.parse(text); } catch (_) { data = { raw: text }; }
    }
    if (!response.ok) {
        throw new Error(`${operationName}失败: ${data.msg || data.message || data.raw || `HTTP ${response.status}`}`);
    }
    return data;
}

function assertFeishuOk(data, operationName) {
    if (data && data.code !== undefined && data.code !== 0) {
        throw new Error(`${operationName}失败: ${data.msg || data.message || `code=${data.code}`}`);
    }
}

async function tenantAccessToken(bridgeConfig = loadBridgeConfig()) {
    const now = Date.now();
    if (
        tenantTokenCache.token &&
        tenantTokenCache.appId === bridgeConfig.appId &&
        tenantTokenCache.appSecret === bridgeConfig.appSecret &&
        tenantTokenCache.expiresAt - now > 60000
    ) {
        return tenantTokenCache.token;
    }

    const data = await postJson('https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal', {
        app_id: bridgeConfig.appId,
        app_secret: bridgeConfig.appSecret,
    }, { operationName: '获取飞书 tenant_access_token' });
    assertFeishuOk(data, '获取飞书 tenant_access_token');
    if (!data.tenant_access_token) throw new Error('获取飞书 tenant_access_token 失败: 响应缺少 token');

    const expire = Number.parseInt(data.expire, 10);
    tenantTokenCache = {
        appId: bridgeConfig.appId,
        appSecret: bridgeConfig.appSecret,
        token: data.tenant_access_token,
        expiresAt: now + Math.max(60, Number.isFinite(expire) ? expire - 180 : 6900) * 1000,
    };
    return tenantTokenCache.token;
}

// v15 (09-19): 协议标记归一化——历史层与展示层共用。
// 关键修复: 历史文件此前存裸 reply, glm-5.3 的漂移形态(缺[/「末»/]?)原样回流上下文 = 模型给自己的
// 错误形态做 few-shot, 漂移越喂越多(VCPChat 无此病: contextSanitizer 深度净化历史, 故无需任何规矩)。
// 本插件走"归一化不折叠": 畸形标记归正为标准形态, 块内容全保留(上下文信息不丢), 从源头断污染环。
function normalizeProtocolMarkers(text) {
    return String(text)
        // v27 (09-23): 第四族漂移——HTML注释包装的工具请求 + 上游"回复重写"模板复读。
        // 实测样本: 09-23 18:37 与 21:0x 炼丹师楼层输出原样复读上游重写中间件请求原文:
        //   <!-- VCP_TOOL_REQUEST --> 流程目标：提供可供重写的下一轮助手回复（目标文本）。
        //   待重写文本： <<<FileOperator 调用成功；LinuxShellExecutor 调用成功。>>>
        //   执行标准：…直接输出重写后的完整回复。
        // 与 v15 工具回执回流同族(模型把上游模板当语料复读); 历史层同剥 = 断 few-shot 污染环。
        .replace(/<!--[ \t]*(?:VCP_)?TOOL_REQUEST[ \t]*-->[\s\S]*?直接输出重写后的完整回复。?[ \t]*/g, '')
        .replace(/<!--[ \t]*(?:VCP_)?TOOL_REQUEST[ \t]*-->/g, '')  // 截断兜底: 结尾锚缺失时至少剥标记
        // v25 (09-21): 第三族漂移——块包装形态/裸块独占行/弯引号参数符/PageBreak 分段符
        // 实测样本: 09-21 08:49 [TOOL_REQUEST_BLOCK_START:ID]/[PageBreak]; 08:55 裸 [TOOL_REQUEST](含行尾内联形态)+“始”/“末”
        .replace(/\[TOOL_REQUEST_BLOCK_START[:\d]*\]/g, '<<<[TOOL_REQUEST]>>>')
        .replace(/\[TOOL_REQUEST_BLOCK_END[:\d]*\]/g, '<<<[END_TOOL_REQUEST]>>>')
        .replace(/\[TOOL_REQUEST\][ \t]*(?=\n|$)/g, '<<<[TOOL_REQUEST]>>>')
        .replace(/\[END_TOOL_REQUEST\][ \t]*(?=\n|$)/g, '<<<[END_TOOL_REQUEST]>>>')
        .replace(/“始”/g, '「始」')
        .replace(/“末”/g, '「末」')
        .replace(/\[PageBreak\]/g, '\n\n')
        .replace(/<<<\[END_TOOL_REQUEST[^\n]{0,4}?>>>/g, '<<<[END_TOOL_REQUEST]>>>')
        .replace(/<<<\[TOOL_REQUEST[^\n]{0,4}?>>>/g, '<<<[TOOL_REQUEST]>>>')
        .replace(/<<<\[ROLE_DIVIDE_USER[^\n]{0,4}?>>>/g, '<<<[ROLE_DIVIDE_USER]>>>')
        .replace(/<<<\[END_ROLE_DIVIDE_USER[^\n]{0,4}?>>>/g, '<<<[END_ROLE_DIVIDE_USER]>>>')
        .replace(/<<<TOOL_REQUEST\]?\??>>>/g, '<<<[TOOL_REQUEST]>>>')
        .replace(/<<<END_TOOL_REQUEST\]?\??>>>/g, '<<<[END_TOOL_REQUEST]>>>')
        .replace(/「末»/g, '「末」')
        // v28 (10-05): 末符第四族变体——T6实锤「末》(U+300B CJK书名号)坏包; 一并覆盖「末〉「末＞及全角引号对
        .replace(/「末[》〉＞>]/g, '「末」')
        .replace(/[『{]始[』}]/g, '「始」')
        .replace(/[『{]末[』}]/g, '「末」');
}
// v16 (09-19): 思维链剥除——VCPChat contextSanitizer 同款"独占行"规则。
// 正文行内 <think> 字面提及原样保留(独占行锚定); 展示层围栏内示例由 v14c stash 保护。
function stripThoughtChains(text) {
    if (typeof text !== "string") return text;
    return text
        .replace(/^[ \t]*\[--- VCP元思考链(?::\s*"[^"]*")?\s*---\][ \t]*\r?\n[\s\S]*?^[ \t]*\[--- 元思考链结束 ---\][ \t]*(?:\r?\n|$)/gm, "")
        .replace(/^[ \t]*<think(?:ing)?>[ \t]*\r?\n[\s\S]*?^[ \t]*<\/think(?:ing)?>[ \t]*(?:\r?\n|$)/gim, "");
}

// v28 (10-05): 工具块扫描器——移植自 VCPChat modules/text-viewer.js 的
// findToolRequestEnd/replaceToolRequestBlocks 状态机(参照实现)。
// 比朴素正则稳健三点: ①ESCAPE字段内嵌 <<<[END_TOOL_REQUEST]>>> 伪标记不提前闭合
// ②反引号包裹的标记是字面示例不折叠 ③「始/末」边界容忍变体。feishuFormatReply 与
// stripOrphanBlocks 共用, 修复坏块/示例块被误折叠导致的格式事故。
const VCPTOOL_START = '<<<[TOOL_REQUEST]>>>';
const VCPTOOL_END = '<<<[END_TOOL_REQUEST]>>>';
function vcpIsBacktickWrapped(source, index, marker) {
    return source[index - 1] === '`' || source[index + marker.length] === '`';
}
function vcpFindMarkedFieldEnd(source, contentStart, isEscape) {
    const re = isEscape ? /[「{]末[Ee][Ss][Cc][Aa][Pp][Ee][」}]/gi : /[「{]末[」}]/g;
    re.lastIndex = contentStart;
    const m = re.exec(source);
    return m ? m.index + m[0].length : source.length;
}
function vcpFindToolRequestEnd(source, contentStart) {
    const markerRegex = /<<<\[END_TOOL_REQUEST\]>>>|[「{]始(?:[Ee][Ss][Cc][Aa][Pp][Ee])?[」}]/gi;
    markerRegex.lastIndex = contentStart;
    while (true) {
        const mm = markerRegex.exec(source);
        if (!mm) return -1;
        const marker = mm[0];
        if (marker === VCPTOOL_END) {
            if (vcpIsBacktickWrapped(source, mm.index, marker)) {
                markerRegex.lastIndex = mm.index + marker.length;
                continue;
            }
            return mm.index + marker.length;
        }
        const isEscape = /escape/i.test(marker);
        markerRegex.lastIndex = vcpFindMarkedFieldEnd(source, mm.index + marker.length, isEscape);
    }
}
// 扫描器版块替换(替代朴素正则); replacer(fullMatch, content) 同 String.replace 回调签名
function vcpReplaceToolBlocks(source, replacer) {
    if (typeof source !== 'string' || !source.includes(VCPTOOL_START)) return source;
    let result = '', cursor = 0;
    while (cursor < source.length) {
        const startIndex = source.indexOf(VCPTOOL_START, cursor);
        if (startIndex === -1) { result += source.slice(cursor); break; }
        if (vcpIsBacktickWrapped(source, startIndex, VCPTOOL_START)) {
            result += source.slice(cursor, startIndex + VCPTOOL_START.length);
            cursor = startIndex + VCPTOOL_START.length;
            continue;
        }
        const contentStart = startIndex + VCPTOOL_START.length;
        const endIndex = vcpFindToolRequestEnd(source, contentStart);
        if (endIndex === -1) { result += source.slice(cursor); break; }
        result += source.slice(cursor, startIndex);
        result += replacer(source.slice(startIndex, endIndex), source.slice(contentStart, endIndex - VCPTOOL_END.length));
        cursor = endIndex;
    }
    return result;
}

// v16: 孤儿协议块剥除(截断生成只有开无结)——展示层与历史层共用(历史层裸存孤儿=漂移温床)
function stripOrphanBlocks(text) {
    let t = String(text);
    // v28: 真孤儿才剥——用扫描器找"最后一个非反引号包裹的START之后是否还有非包裹的END"。
    // 旧正则不认反引号字面示例, 会把 `<<<[TOOL_REQUEST]>>>` 示例当孤儿, 吞掉其后全部正文
    // (T7实锤: 686字回复清洗后只剩'`')。反引号包裹的标记跳过; 结果块孤儿逻辑不变。
    let lastRealStart = -1;
    for (let ci = 0; ci < t.length; ) {
        const si = t.indexOf(VCPTOOL_START, ci);
        if (si === -1) break;
        if (!vcpIsBacktickWrapped(t, si, VCPTOOL_START)) lastRealStart = si;
        ci = si + VCPTOOL_START.length;
    }
    if (lastRealStart >= 0) {
        // 从真START起, 找非包裹的END; 找不到才剥尾部
        let hasClose = false;
        for (let ci = lastRealStart; ci < t.length; ) {
            const ei = t.indexOf(VCPTOOL_END, ci);
            if (ei === -1) break;
            if (!vcpIsBacktickWrapped(t, ei, VCPTOOL_END)) { hasClose = true; break; }
            ci = ei + VCPTOOL_END.length;
        }
        if (!hasClose) t = t.slice(0, lastRealStart);
    }
    return t
        .replace(/\[\[VCP调用结果信息汇总:(?![\s\S]*?VCP调用结果结束\]\])[\s\S]*$/g, "");
}

// v26 (09-21): 退行指纹熔断——09:23 采样崩塌事故(38266 token/597s 退行循环 + vcpLoop 4轮回灌自激)的检测器。
// 判据(尾窗6K): R1 精确循环 = 单元2-24背靠背≥4次, 含字词字符, 块≥48; R1b 骨架循环 = 剥标点空白后同左(块≥16), 专杀变体复读;
//   R2 复读行 = 同一非平凡行(6-120字符)≥5次 ∧ 尾窗比率<0.25; R3 熵坍塌 = 比率<0.16 ∧ 首行重复≥3(熵不单独定罪, JSON实测0.115)。
// 误伤面评估: 正常JSON/表格工具输出比率≥0.25不触发; 心跳/日志类≥0.3; 连续相同行×4的"正常"输出本身即病态, 可接受。
// 终稿守卫另加全文压缩率<0.15(3000字符起测)——56K崩塌实测由该规则兜底命中。
const zlib = require('zlib');
function __v26SkeletonCycle(text) {
    const skel = String(text || '').replace(/[^0-9A-Za-z\u4e00-\u9fff]/g, '');
    const RES = /([0-9A-Za-z\u4e00-\u9fff]{2,16}?)\1{3,}/g;
    let ms;
    while ((ms = RES.exec(skel)) !== null) {
        if (ms[0].length >= 16) return ms[1];
        RES.lastIndex = ms.index + 1;
    }
    return null;
}
function __v26TopRepeatLine(tail) {
    const lc = Object.create(null);
    let topN = 0;
    for (const ln of tail.split(/\r?\n/)) {
        const k = ln.trim();
        if (k.length < 6 || k.length > 120) continue;
        const n = (lc[k] = (lc[k] || 0) + 1);
        if (n > topN) topN = n;
    }
    return topN;
}
function degenerateTailAnalysis(text) {
    const t = String(text || '');
    if (t.length < 3000) return { hit: false };
    const WIN = 6000;
    const tail = t.length > WIN ? t.slice(-WIN) : t;
    // R1 连续复读: 全局多轮扫描(惰性首匹配会先撞上正文 \n\n 类单字符复读而漏掉真复读)。
    // 单元须含字词字符(字母/数字/CJK, 排除 ---\n / \n\n 类纯符号行), 复读块≥48字符(≥4次×12字符, 排除短促修辞重复)。
    const RE = /([\s\S]{2,24}?)\1{3,}/g;
    let m;
    while ((m = RE.exec(tail)) !== null) {
        if (m[0].length >= 48 && /[0-9A-Za-z\u4e00-\u9fff]/.test(m[1])) {
            return { hit: true, rule: 'R1-cycle', sample: m[1] };
        }
        RE.lastIndex = m.index + 1; // 重叠推进: 被守卫拒绝的匹配后移一位继续找真单元
    }
    // R1b 骨架复读(09-21 事故实测: 变体复读"测定: NOISE/测定 NOISE/测定: NOISE "在原始文本上无精确循环,
    // 剥掉标点空白后坍缩为同一骨架串×N 现形)。骨架=仅字词字符; 单元2-16, 块≥16骨架字符。干净语料实测零命中。
    const sk = __v26SkeletonCycle(tail);
    if (sk) return { hit: true, rule: 'R1b-skeleton', sample: sk };
    // 熵坍塌不能单独定罪(结构化JSON压缩率可低至0.11), 须线级重复佐证
    const ratio = zlib.gzipSync(Buffer.from(tail, 'utf8')).length / Buffer.byteLength(tail, 'utf8');
    if (ratio >= 0.25) return { hit: false };
    const topN = __v26TopRepeatLine(tail);
    if (ratio < 0.16 && topN >= 3) return { hit: true, rule: 'R3-entropy', sample: tail.slice(0, 24) };
    if (topN >= 5) return { hit: true, rule: 'R2-lines', sample: tail.slice(0, 24) };
    return { hit: false };
}
function isDegenerateText(text) {
    const t = String(text || '');
    if (t.length < 3000) return false;
    if (degenerateTailAnalysis(t).hit) return true;
    // 全文骨架扫(不看窗): 崩塌带任意位置出现即命中——终稿守卫的最后防线
    if (__v26SkeletonCycle(t)) return true;
    const r = zlib.gzipSync(Buffer.from(t, 'utf8')).length / Buffer.byteLength(t, 'utf8');
    return r < 0.15 && __v26TopRepeatLine(t) >= 3; // 全文熵坍塌同样须线级佐证(JSON防误伤)
}
function buildDegenerateStub(raw, keepPrefix) {
    const r = String(raw || '');
    // v26.1 blast radius 修复: 流内看门狗命中是"局部退行"(尾窗), 退行前的主体可能是干净分析——
    // 前缀自检(去尾6.4K后 R1/R1b/复读行 再查一遍)通过则保留主体, 不通过(全文烂, 如56K全程崩塌)才全核爆。
    if (keepPrefix && r.length > 4000) {
        // 梯式回退: 退行区可能只有几十字符(局部结巴), 固定大边距会误砍干净主体。
        // 逐级试 [1200,3200,6400,12800], 取首个通过强检测(含全文骨架扫, 防56K垃圾前缀漏判)的最大干净前缀。
        for (const margin of [1200, 3200, 6400, 12800]) {
            if (r.length - margin < 2000) continue;
            let body = r.slice(0, r.length - margin);
            const lb = body.lastIndexOf('\n');
            if (lb > 200) body = body.slice(0, lb);
            if (body.length > 2000 && !isDegenerateText(body)) {
                return `[存根-v26.1] 回复尾部检测到局部退行(采样崩塌防护), 已截除退行段(原长${r.length}字符, 保留主体${body.length}字符)。以下为退行前的干净主体:\n\n${body}\n\n[存根尾] 以上主体未受污染可正常采信; 退行段已截除, 勿模仿其格式。`;
            }
        }
    }
    const head = r.replace(/\s+/g, ' ').trim().slice(0, 160);
    return `[存根-v26] 原回复(${r.length}字符)触发退行指纹熔断(采样崩塌防护), 已作废存根化, 勿模仿其中任何格式与标记。原回复开头: ${head}`;
}

// v16: 上下文预算裁剪——从最新往回装, 超预算丢最旧, 最近 minKeep 条保底。纯函数便于单测。
// token 估算 chars/1.5(中文保守=高估, 宁早裁勿爆窗)。VCPChat 无此件(同病), 此为 feishuBot 先行。
function trimMessagesToBudget(messages, budgetTokens, reserveTokens, minKeep) {
    const CHARS_PER_TOKEN = 1.5;
    const est = (m) => Math.ceil(String((m && m.content) || "").length / CHARS_PER_TOKEN);
    let used = Math.max(0, Number(reserveTokens) || 0);
    const kept = [];
    for (let i = messages.length - 1; i >= 0; i--) {
        const t = est(messages[i]);
        if (kept.length >= (minKeep || 6) && used + t > budgetTokens) break;
        kept.unshift(messages[i]);
        used += t;
    }
    return kept;
}
// 飞书显示层格式化：把 VCP 协议块折叠为单行摘要（仅作用于发送给用户的消息，
// 历史文件仍存原始 reply，保证 Agent 下轮上下文完整）
function feishuFormatReply(text) {
    if (!text) return text;
    // ---- v14c (09-19): 代码围栏域保护（VCPChat messageRenderer 保护-恢复骨架同款）----
    // 围栏内的协议标记是字面示例: 不折叠/不归一化/不被兜底剥除; 末尾 LIFO split/join 恢复(防 $& 特殊替换)
    const fenceStash = [];
    // v30 (10-05): 行内反引号域一并保护(VCPChat markdownCodeDomainScanner同款语义)——
    // A/B对比实锤: 行内字面示例 `<<<[TOOL_REQUEST]>>>` 被兜底剥成空反引号。
    // 规则: 围栏优先(```成对), 行内= 同行内的 `...`(内容不含反引号/换行); 孤立未闭合反引号不保护。
    text = String(text).replace(/(```[\s\S]*?```)|(`[^`\n]+`)/g, (fm) => {
        fenceStash.push(fm);
        return '__VCP_FENCE_' + (fenceStash.length - 1) + '__';
    });
    // v14 (09-19): 折叠产物(🔧/✅/❌/📋)独立成行——协议块嵌在段落中间时, 原位替换会把折叠行粘在正文段尾
    const ownLine = (off, len, full, s) => {
        // v18: 段落级拆分——工具行前后各保证一个空行(用户三连纠偏: v14单换行不够, 要拆段)
        const pre = off > 0 ? '\n\n' : '';
        const post = off + len < full.length ? '\n\n' : '';
        return pre + s + post;
    };

    // ---- v16: 思维链剥除(展示层也剥——reasoning 不该给用户看; 围栏已在上方被 stash 保护) ----
    text = stripThoughtChains(text);
    // ---- v9+v12+v16: 归一化 + 孤儿剥除 → 共用函数(与历史层同源) ----
    text = normalizeProtocolMarkers(text);
    text = stripOrphanBlocks(text);

    // ---- v3: 参数瘦身 + 请求/结果配对 + 敏感参数打码 + 结果带产出摘要 ----
    const SENSITIVE = /^(requireAdmin|.*(?:apikey|api_key|key|token|secret|password|authcode).*)$/i;
    const NOISE = new Set(['hostId', 'timeout', 'isLongRunning', 'outputFormat', 'doubleConfirm', 'maid', 'max_results']);
    const cap = (s, n) => { s = String(s).replace(/\s+/g, ' ').trim(); return s.length > n ? s.slice(0, n) + '…' : s; };
    const baseName = (p) => { const segs = String(p).replace(/\\/g, '/').split('/').filter(Boolean); return segs.length ? segs[segs.length - 1] : String(p); };

    const fmtParam = (k, v) => {
        if (k === 'command') return cap(v, 48);
        if (k === 'filePath' || k === 'directoryPath' || k === 'path' || k === 'folder') return baseName(v);
        if (k === 'content') return '「' + cap(v, 60) + '」';
        if (k === 'query') return cap(v, 30);
        return cap(v, 24);
    };

    // 1. 工具调用块 → 一行摘要（按出现顺序登记，供结果行配对）
    const reqTags = [];
text = vcpReplaceToolBlocks(text, (m, body) => {
        const params = {};
        body.replace(/([A-Za-z_]+)\s*:\s*「始」([\s\S]*?)「末」/g, (m2, k, v) => { params[k] = v.trim(); return m2; });
        const tool = params.tool_name || '未知工具';
        const args = Object.keys(params)
            .filter(k => k !== 'tool_name' && !NOISE.has(k) && !SENSITIVE.test(k))
            .map(k => k + '=' + fmtParam(k, params[k]))
            .join(' ');
        const tag = tool + (args ? ' · ' + args : '');
        reqTags.push({ tool, tag });
        return '\n\n🔧 ' + tag + '\n\n';
    });

    // 2. 工具结果块 → 与请求配对；成功带一行产出，失败带一句原因
    let ridx = 0;
    // v32: 收集卡片素材(供即时工具卡用)
    const cardFacts = (typeof feishuFormatReply.cardFacts === 'object') ? feishuFormatReply.cardFacts : (feishuFormatReply.cardFacts = []);
    cardFacts.length = 0;
    text = text.replace(/\[\[VCP调用结果信息汇总:([\s\S]*?)VCP调用结果结束\]\]/g, (m, body, off, full) => {
        const tool = ((body.match(/工具名称:\s*(.+)/) || [])[1] || '未知工具').trim();
        let tag = null;
        if (reqTags[ridx] && reqTags[ridx].tool === tool) { tag = reqTags[ridx].tag; ridx++; }
        else { const j = reqTags.findIndex((t, i) => i >= ridx && t.tool === tool); if (j >= 0) { tag = reqTags[j].tag; ridx = j + 1; } }
        const label = tag || tool;
        const isErr = body.includes('❌ ERROR') || body.includes('执行状态: error');
        const content = (body.split(/返回内容:\s*/)[1] || '').trim();
        if (!isErr) {
            let hint = '';
            try { const j = JSON.parse(content); const r = j.result || j; hint = r.output || r.message || j.message || ''; } catch (e) { hint = content; }
            cardFacts.push({ tool, argsText: (label !== tool ? label.slice(tool.length + 3) : ''), ok: true, hint: cap(String(hint).split('\n')[0], 36), detail: content });
            hint = String(hint).split('\n').map(s => s.trim()).filter(Boolean)[0] || '';
            if (/^[\{\[]/.test(hint)) hint = '';
            hint = cap(hint, 36);
            return ownLine(off, m.length, full, '✅ ' + label + (hint ? ' → ' + hint : ''));
        }
        let reason = '';
        try { const j = JSON.parse(content); reason = j.error || j.message || ''; } catch (e) {
            const em = content.match(/"error"\s*:\s*"([^"]+)"/);
            reason = em ? em[1] : content;
        }
        cardFacts.push({ tool, argsText: (label !== tool ? label.slice(tool.length + 3) : ''), ok: false, hint: cap(String(reason).replace(/\\n/g, ' ').split('。')[0], 36), detail: content });
        reason = String(reason).replace(/^执行错误:\s*/, '').replace(/\\n/g, ' ').split(/[。\n]/)[0].slice(0, 500).trim();
        return ownLine(off, m.length, full, '❌ ' + label + (reason ? ' — ' + reason : ''));
    });

    // 3. ROLE_DIVIDE 剥壳（内部块已在上面处理）
    text = text.replace(/<<<\[ROLE_DIVIDE_\w+\]>>>/g, '');
    text = text.replace(/<<<\[END_ROLE_DIVIDE_\w+\]>>>/g, '');

    // 4. 本轮工具调用摘要 → 整块删除(v32.3修复: 此前误把replace整句删掉导致摘要原样泄漏;
    //    明细已在interactive卡/⚡卡里, 不再生成计数行)
    text = text.replace(/\[本轮工具调用摘要:[\s\S]*?本轮工具调用摘要结束\]/g, '');
    // v32.3: "块前置——/块前置："开头的宣告残行剥除(工具活动已在卡里, 正文不再需要前置声明)
    text = text.replace(/^\s*块前置[——:：]?[^\n]*$/gm, '');

    // 5. 兜底：其他协议残留
    text = text.replace(/\[\[VCP通过多种方式[\s\S]*?\]\]/g, '');
    text = text.replace(/<<<\[(?:END_)?[A-Z_]+(?:_[A-Z]+)*\]>>>/g, '');

    // v9 兜底: 折叠后仍残留的协议残段(未知变体)整段剥除, 不让 <<<[ 裸奔
    text = text.replace(/<<<\[[\s\S]{0,400}?>>>/g, '').replace(/<<<\[[^\n]{0,200}/g, '');

    text = text.replace(/\n{3,}/g, '\n\n');
    // v14c: 恢复围栏原文（LIFO 逆序, split/join 防 $ 特殊替换模式——VCPChat 同款）
    for (let i = fenceStash.length - 1; i >= 0; i--) {
        text = text.split('__VCP_FENCE_' + i + '__').join(fenceStash[i]);
    }
    return text.trim();
}



// ---- v32: Mermaid 服务端渲染管线 (chromium+puppeteer-core, scripts/renderMeridian.js) ----
const { execFile } = require('child_process');
async function renderMermaidToTemp(code) {
    const os = require('os');
    const fs = require('fs');
    const tmp = os.tmpdir();
    const id = 'mmd_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
    const mmd = path.join(tmp, id + '.mmd');
    const png = path.join(tmp, id + '.png');
    fs.writeFileSync(mmd, code);
    return new Promise((resolve) => {
        // v32.3修复: 脚本在VCPToolBox/scripts(=插件目录../../scripts), 此前../scripts指向
        // Plugin/scripts不存在, execFile报错被静默吞成null → 围栏被剥但图没发
        const mmdScript = fs.existsSync(path.join(__dirname, '..', '..', 'scripts', 'renderMermaid.js'))
            ? path.join(__dirname, '..', '..', 'scripts', 'renderMermaid.js')
            : path.join(__dirname, '..', 'scripts', 'renderMermaid.js'); // 兜底: 其他安装结构
        execFile('node', [mmdScript, mmd, png],
            { timeout: 30000, maxBuffer: 1024 * 1024 }, (err, stdout) => {
                try {
                    if (err) { console.error('[mermaid] render fail:', String(err.message).slice(0, 120)); return resolve(null); }
                    const r = JSON.parse(stdout);
                    if (!r.ok) console.error('[mermaid] script said not-ok:', String(stdout).slice(0, 200));
                    resolve(r.ok ? png : null);
                } catch { resolve(null); }
            });
    });
}

async function uploadFeishuImage(pngPath) {
    const fs = require('fs');
    const bridgeConfig = loadBridgeConfig();
    const token = await tenantAccessToken(bridgeConfig);
    const boundary = '----vcpboundary' + Date.now();
    const meta = `--${boundary}\r\nContent-Disposition: form-data; name="image_type"\r\n\r\nmessage\r\n` +
        `--${boundary}\r\nContent-Disposition: form-data; name="image"; filename="diagram.png"\r\n` +
        `Content-Type: image/png\r\n\r\n`;
    const body = Buffer.concat([Buffer.from(meta, 'utf8'), fs.readFileSync(pngPath), Buffer.from(`\r\n--${boundary}--\r\n`, 'utf8')]);
    // v32: multipart原生fetch(Buffer body), postJson只会JSON.stringify
    const resp = await fetchWithTimeout('https://open.feishu.cn/open-apis/im/v1/images', {
        method: 'POST',
        headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'multipart/form-data; boundary=' + boundary },
        body,
    }, FEISHU_HTTP_TIMEOUT_MS, '上传飞书图片');
    const data = await resp.json().catch(() => ({}));
    if (data.code !== 0 || !data.data || !data.data.image_key) throw new Error('图片上传失败: ' + JSON.stringify(data).slice(0, 120));
    return data.data.image_key;
}

// 正文投递前钩子: ```mermaid 围栏 → 渲染PNG → 发image消息, 返回剥掉围栏后的正文
async function drainMermaidBlocks(target, text, { replyToMessageId = '' } = {}) {
    const fenceRe = /```(?:mermaid|flowchart|graph)\s*\n([\s\S]*?)```/g;
    const jobs = [];
    let m;
    while ((m = fenceRe.exec(text)) !== null) jobs.push({ m: m[0], code: m[1] });
    if (!jobs.length) return text;
    for (const job of jobs) {
        try {
            const png = await renderMermaidToTemp(job.code);
            if (!png) { warn('[mermaid] 渲染返回null, 围栏将被剥除但无图'); continue; }
            const imageKey = await uploadFeishuImage(png);
            const bridgeConfig = loadBridgeConfig();
            const token = await tenantAccessToken(bridgeConfig);
            await postJson('https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=' + (String(target).startsWith('oc_') ? 'chat_id' : 'open_id'),
                { receive_id: target, msg_type: 'image', content: JSON.stringify({ image_key: imageKey }) },
                { headers: { Authorization: 'Bearer ' + token } });
            require('fs').unlink(png, () => {});
            log('[mermaid] 已渲染并投递流程图 image_key=' + imageKey);
        } catch (e) { warn('mermaid渲染投递失败(保留原文):', e.message); continue; }
    }
    return text.replace(fenceRe, '').replace(/\n{3,}/g, '\n\n').trim();
}

// ---- v32: 工具活动 interactive 卡片(collapsible_panel 真折叠, 卡片JSON 2.0) ----
// VCPChat 对标: vcp-tool-use-bubble + "点击展开全部"; 飞书等价物 = header摘要+折叠详情面板。
// 已实测(10-06): schema 2.0 + collapsible_panel(expanded:false) 客户端原生折叠, 无需回调服务。
function buildToolCard({ tool, argsText, ok, hint, detail, error }) {
    const lang = (tool === 'LinuxShellExecutor' || /shell|bash/i.test(tool)) ? 'bash' : 'text';
    const headerTpl = ok === undefined ? 'turquoise' : (ok ? 'green' : 'red');
    const icon = ok === undefined ? '🔧' : (ok ? '✅' : '❌');
    const els = [];
    if (argsText) els.push({ tag: 'markdown', content: '**参数**\n```' + lang + '\n' + argsText + '\n```' });
    if (error) els.push({ tag: 'markdown', content: '**错误**\n```text\n' + String(error).slice(0, 1500) + '\n```' });
    els.push({
        tag: 'collapsible_panel', expanded: false,
        header: { title: { tag: 'plain_text', content: '▶ 展开完整结果' } },
        elements: [{ tag: 'markdown', content: '```text\n' + String(detail || hint || '(无输出)').slice(0, 3000) + '\n```' }],
    });
    return {
        schema: '2.0',
        header: { title: { tag: 'plain_text', content: icon + ' ' + tool + (ok === undefined ? ' · 执行中' : (ok ? ' · 成功' : ' · 失败')) },
                  subtitle: hint ? { tag: 'plain_text', content: String(hint).slice(0, 60) } : undefined,
                  template: headerTpl },
        body: { elements: els },
    };
}

async function sendToolCard(target, card, { replyToMessageId = '' } = {}) {
    // 复用 sendFeishuText 的凭证/回复管线: interactive 类型即卡片消息
    return sendFeishuText(target, card, { replyToMessageId, forceInteractive: true });
}

// ---- v4: Hermes feishu 同款消息模板 (表格→text / Markdown→post 富文本 / 纯文本→text) ----
const MD_TABLE_RE = /\|.*\|/;
const MD_HINT_RE = /(^|\n)(#{1,6}\s|\s*[-*]\s|\s*\d+\.\s|\s*---+$|\s*>)|```|`[^`\n]+`|\*\*[^*\n].*?\*\*|~~[^~\n].*?~~|\*[^*\n]+\*|\[[^\]]+\]\([^\)]+\)/;

function buildMarkdownPostRows(text) {
    const rows = [];
    const parts = String(text).split(/(```[\s\S]*?```)/g); // 代码块必须独立 row，否则飞书 md 渲染器吞内容
    for (const part of parts) {
        if (!part) continue;
        if (part.startsWith('```')) { rows.push([{ tag: 'md', text: part }]); continue; }
        // v7a: 工具行(🔧/✅/❌/📋)包成 code 行, 原样保真(09-16 用户要求)
        const TOOL_LINE_RE = /^\s*(?:⚡|🔧|✅|❌|📋)/; // v10.3: +⚡（即时工具卡行）
        // v7b: 管道表格块(|...|连续行)整体转一个 code row——md tag 不渲染表格, 拆行会碎
        const TABLE_LINE_RE = /^\s*\|.*\|\s*$/;
        const lines = part.split('\n');
        let i = 0;
        while (i < lines.length) {
            const line = lines[i];
            if (!line.trim()) { i++; continue; }
            if (TABLE_LINE_RE.test(line)) {
                const block = [];
                while (i < lines.length && TABLE_LINE_RE.test(lines[i])) { block.push(lines[i]); i++; }
                // v8: 表格整块原文进单个 md row——post md 标签真渲染表格(A/B 实测 09-16),
                // 勿包```代码块(那会显示为代码文本)
                rows.push([{ tag: 'md', text: block.join('\n') }]);
                continue;
            }
            if (TOOL_LINE_RE.test(line)) {
                rows.push([{ tag: 'md', text: '`' + line.trim() + '`' }]);
                i++;
                continue;
            }
            rows.push([{ tag: 'md', text: line }]);
            i++;
        }
    }
    return rows;
}

function buildOutboundPayload(text) {
    const t = String(text);
    // v7: 表格不再强制降级 text——若同时含 md 特征走 post 渲染; 表格本身不是飞书 md 白名单语法,
    // buildMarkdownPostRows 会把含管道表的连续块整体转 code row 保真(09-16 用户要求富文本优先)
    if (MD_HINT_RE.test(t)) {
        return { msg_type: 'post', content: JSON.stringify({ zh_cn: { content: buildMarkdownPostRows(t) } }) };
    }
    return { msg_type: 'text', content: JSON.stringify({ text: t }) };
}

// v6: 超长消息分段——段落/行边界切片，代码块```整块不拆(块超限才硬切)，表格行=单行保持完整
function splitForFeishu(text, maxChars) {
    // v13 (09-19): 原子块保护升级——工具块/代码块/结果块整块不拆。
    // v6 只护 ``` 和标准 TOOL_REQUEST；v12 归一化后畸形变体也已归一，此处统一拦截。
    // 单原子块超上限: 拒绝硬切 → 整块落盘暂存 + 返回占位段提示人工取。
    const t0 = String(text);
    if (t0.length <= maxChars) return [t0];
    const ATOMIC_ALL = /(```[\s\S]*?```|<<<\[TOOL_REQUEST\]>[\s\S]*?<<<\[END_TOOL_REQUEST\]>>>|\[\[VCP调用结果信息汇总:[\s\S]*?VCP调用结果结束\]\])/g;
    for (const m of t0.match(ATOMIC_ALL) || []) {
        if (m.length > maxChars) {
            let dumpPath = '';
            try {
                const os = require('os');
                dumpPath = path.join(os.tmpdir(), 'feishu_oversize_' + Date.now() + '.txt');
                fs.writeFileSync(dumpPath, m, 'utf8');
            } catch (e) { dumpPath = '(落盘失败: ' + e.message + ')'; }
            log('v13 原子块超限拒切: ' + m.length + ' 字符 → ' + dumpPath);
            const head = m.slice(0, 80).replace(/\n/g, ' ');
            const placeholder = '⚠️ 单个工具/代码块 ' + m.length + ' 字符超段限，已整体暂存: ' + dumpPath + ' （内容开头: ' + head + '…）';
            const rest = t0.replace(m, '');
            const restSegs = splitForFeishu(rest, maxChars);
            return [...restSegs.slice(0, 0), placeholder, ...restSegs];
        }
    }
    const t = t0;
    const segs = [];
    let cur = '';
    const pushSeg = () => { if (cur.trim()) segs.push(cur.trim()); cur = ''; };
    const pushHard = (line) => { for (let k = 0; k < line.length; k += maxChars) segs.push(line.slice(k, k + maxChars)); };
    const ATOMIC_RE = /(```[\s\S]*?```|<<<\[TOOL_REQUEST\]>[\s\S]*?<<<\[END_TOOL_REQUEST\]>>>)/g;
    for (const part of t.split(ATOMIC_RE)) {
        if (!part) continue;
        if (part.startsWith('```') || part.startsWith('<<<[TOOL_REQUEST]>>>')) { // 代码块/工具块整块处理
            if (part.length > maxChars) { pushSeg(); pushHard(part); }
            else if ((cur ? cur + '\n' + part : part).length > maxChars) { pushSeg(); segs.push(part); }
            else { cur = cur ? cur + '\n' + part : part; }
            continue;
        }
        for (const para of part.split(/\n\s*\n/)) { // 空行段落边界
            const p = para.trim();
            if (!p) continue;
            if (p.length > maxChars) { // 单段超限 → 行边界切，超长行硬切
                pushSeg();
                let lineBuf = '';
                for (const line of p.split('\n')) {
                    if (line.length > maxChars) {
                        if (lineBuf.trim()) { segs.push(lineBuf.trim()); lineBuf = ''; }
                        pushHard(line);
                    } else if ((lineBuf ? lineBuf + '\n' + line : line).length > maxChars) {
                        if (lineBuf.trim()) segs.push(lineBuf.trim());
                        lineBuf = line;
                    } else {
                        lineBuf = lineBuf ? lineBuf + '\n' + line : line;
                    }
                }
                if (lineBuf.trim()) segs.push(lineBuf.trim());
            } else if ((cur ? cur + '\n\n' + p : p).length > maxChars) {
                pushSeg(); cur = p;
            } else {
                cur = cur ? cur + '\n\n' + p : p;
            }
        }
    }
    pushSeg();
    return segs.length ? segs : [t.slice(0, maxChars)];
}

async function sendFeishuText(target, text, { replyToMessageId = '', receiveIdType = '', forcePost = false, forceInteractive = false } = {}) {
    const bridgeConfig = loadBridgeConfig();
    if (!bridgeConfig.appId || !bridgeConfig.appSecret) throw new Error('缺少飞书凭证，无法发送消息');
    const token = await tenantAccessToken(bridgeConfig);

    const sendOnce = async (msgType, content) => {
        if (replyToMessageId) {
            const data = await postJson(
                `https://open.feishu.cn/open-apis/im/v1/messages/${encodeURIComponent(replyToMessageId)}/reply`,
                { msg_type: msgType, content },
                { headers: { Authorization: `Bearer ${token}` }, operationName: '回复飞书消息' }
            );
            assertFeishuOk(data, '回复飞书消息');
            return data;
        }
        const idType = inferReceiveIdType(target, receiveIdType);
        const data = await postJson(
            `https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=${encodeURIComponent(idType)}`,
            { receive_id: target, msg_type: msgType, content },
            { headers: { Authorization: `Bearer ${token}` }, operationName: '发送飞书消息' }
        );
        assertFeishuOk(data, '发送飞书消息');
        return data;
    };

    // v6 分段（09-16）：超长文本按段落边界切片多条顺序发送，段尾 (i/N) 标记
    const maxSeg = Number(getConfigValue('FeishuMaxSegmentChars')) > 0
        ? Number(getConfigValue('FeishuMaxSegmentChars')) : 2800;
    const segments = splitForFeishu(text, maxSeg);
    const sendRaw = async (t) => {
        // v10.3: forcePost（⚡工具卡）强制富文本——工具行(🔧/✅/❌/⚡)包 code 行渲染，绕过 buildOutboundPayload 的纯文本判定
        // v32: forceInteractive — 工具卡(collapsible_panel)直通, card对象即content
        const payload = forceInteractive
            ? { msg_type: 'interactive', content: typeof text === 'string' ? text : JSON.stringify(text) }
            : forcePost
            ? { msg_type: 'post', content: JSON.stringify({ zh_cn: { content: buildMarkdownPostRows(t) } }) }
            : buildOutboundPayload(t);
        try {
            return await sendOnce(payload.msg_type, payload.content);
        } catch (e) {
            if (payload.msg_type === 'post') {
                return await sendOnce('text', JSON.stringify({ text: String(t) })); // post 被拒 → 降级纯文本
            }
            throw e;
        }
    };
    if (segments.length <= 1) return sendRaw(text);
    log(`分段发送: ${String(text).length} 字符 → ${segments.length} 段 (上限 ${maxSeg})`);
    let last = null;
    for (let i = 0; i < segments.length; i++) {
        if (i > 0) await new Promise(r => setTimeout(r, 400)); // 顺序发送防乱序+限频
        last = await sendRaw(`${segments[i]}
（${i + 1}/${segments.length}）`);
    }
    return last;
}


// ---- v10.2: 消息状态监听层 ----
// 活跃回复登记表：chatId -> { targetId, messageId, text, since, reported }
const inflight = new Map();
let watchdogTimer = null;

function inflightWatchdogTick() {
    const cfg = loadBridgeConfig();
    const now = Date.now();
    for (const [chatId, st] of inflight) {
        const age = now - st.since;
        if (age < cfg.inflightTimeoutMs) continue;
        const mins = Math.round(age / 60000);
        st.reportCount = (st.reportCount || 0) + 1;
        warn(`[监听] 消息处理中已 ${mins}min（第${st.reportCount}次心跳）: ${st.text.slice(0, 40)}`);
        // v10.4: 无超时杀任务——只要在跑就一直等，每10分钟心跳通报一次
        const msg = st.reportCount === 1
            ? `⏱️ [状态] 你 ${mins} 分钟前的消息（"${st.text.slice(0, 30)}…"）仍在处理中。已取消自动超时——只要还在跑就一直等，工具活动会实时以 ⚡ 卡推送。若确认死掉，重发一次即可（原消息已进历史，不会丢）。`
            : `⏱️ [状态] 仍在处理，累计约 ${mins} 分钟（第 ${st.reportCount} 次心跳）。继续等待中；需要放弃就重发。`;
        sendFeishuText(st.targetId, msg,
            { replyToMessageId: st.messageId }).catch(e => warn('[监听] 状态卡发送失败:', e.message));
        st.since = now; // 重新计时：再过 inflightTimeoutMs 仍无回复才通报下一次
    }
}

// v21 (09-20): 定时任务结果回送 watcher——VCPTimedResults/ 新结果文件出现时,
// 把结果摘要作为一条用户消息注入绑定 Agent 的最新飞书 topic, 触发一次真实 Agent 轮
// (修复: taskScheduler 执行后只落盘+广播 VCPLog, 飞书链路无人消费 = 炼丹师"定时器从未触发"的真相)
const TIMED_RESULTS_DIR = path.join(__dirname, '..', '..', 'VCPTimedResults');
let timedResultsWatcher = null;
const deliveredTimedResults = new Set(); // 防重投(文件未删除时 watcher 重启会重扫)

function summarizeTimedResult(data) {
    const status = data.status === 'success' ? '成功' : '失败';
    let brief = '';
    if (typeof data.resultSummary === 'string' && data.resultSummary) {
        brief = data.resultSummary.slice(0, 600);
    } else if (data.error) {
        brief = String(data.error).slice(0, 600);
    }
    return `定时任务 ${data.taskId} (${data.toolName}) 执行${status}。\n${brief}`;
}

// v23: 系统触发轮的一等入口(TimedRelay/看门狗/webhook 共用)。
// 与用户消息门链平行(范本: AgentAssistant __vcp_timed_call 一等公民设计):
// 系统轮不是用户消息, 不过 去重/@提及/白名单 门; 但 watchdog(markInflight)与
// 完整 agent 轮(历史/流式/踢/格式化/投递)经 handleFeishuEvent 全量共享。
async function handleSystemTurn(text, opts = {}) {
    const bridgeConfig = loadBridgeConfig();
    const agent = findAgent(bridgeConfig.bindAgent);
    if (!agent) throw new Error('[SystemTurn] 找不到绑定 Agent');
    const latestConfig = readJson(agent.configPath, agent.config) || {};
    const allTopics = Array.isArray(latestConfig.topics) ? latestConfig.topics : [];
    // v27: 目标路由——opts.targetId(oc_/ou_)时按topic的_metadata.chatId/targetId匹配,
    // 命中则路由到该群; 未命中或未指定回落topics[0](旧语义)。修复多群Agent系统轮串台。
    let topic = allTopics[0] || null;
    if (opts.targetId) {
        const wanted = String(opts.targetId);
        const hit = allTopics.find(t => {
            const m = (t && t._metadata) || {};
            return String(m.chatId || '') === wanted || String(m.targetId || '') === wanted
                || String(t.targetId || '') === wanted;
        });
        if (!hit) throw new Error(`[SystemTurn] 指定目标 ${wanted} 不在该 Agent 的任何 topic 中——拒绝回落topics[0]防串台`);
        topic = hit;
    }
    const meta = topic?._metadata || {};
    const targetId = opts.targetId || meta.targetId || meta.chatId;
    if (!targetId) throw new Error('[SystemTurn] 最新 topic 无飞书投递目标');
    log(`[SystemTurn] 系统轮: target=${targetId} sender=${opts.senderName || '系统'} text=${String(text).slice(0, 60)}`);
    const session = {
        type: 'text', chatType: meta.chatType || 'group',
        targetId, receiveIdType: meta.receiveIdType || 'chat_id',
        sessionKey: meta.sessionKey, chatId: meta.chatId || null,
        senderId: meta.userId || targetId, senderName: opts.senderName || '系统',
        messageId: null, text: String(text), _system: true,
    };
    return handleFeishuEvent(session);
}

// v27: 属主过滤——task 声明了 maid/agent_name 且不是本插件绑定的 Agent 时静默跳过。
// 结构修复: 双插件(VCPFeishu/VCPFeishuXiaoying)watch同一VCPTimedResults目录,
// 此前每个task被两边各注入一次,靠"小影无topic"的副作用兜底——那是碰巧不是设计。
function timedTaskOwner(data) {
    const args = (data && data.arguments) || {};
    return String(args.maid || args.agent_name || args.agent || '').trim();
}

function deliverTimedResultFile(fp) {
    try {
        const raw = fs.readFileSync(fp, 'utf8');
        const data = JSON.parse(raw);
        // v27: 属主过滤(先于去重标记——非属主task不占key)
        const owner = timedTaskOwner(data);
        const bridgeConfig = loadBridgeConfig();
        if (owner && bridgeConfig.bindAgent && owner !== bridgeConfig.bindAgent) {
            debug(`[TimedRelay] 跳过非属主task: task=${data.taskId} owner=${owner} 本插件=${bridgeConfig.bindAgent}`);
            return;
        }
        const key = data.taskId + '|' + data.executedAt;
        if (deliveredTimedResults.has(key)) return;
        deliveredTimedResults.add(key);
        const msg = `[定时收账 v23] \n${summarizeTimedResult(data)}\n(这是 ${data.toolName} 定时任务的自动回执, 请按收账纪律核对: stdout log + 盘上 JSON + mtime)`;
        log(`[TimedRelay] 定时结果回送: task=${data.taskId}`);
        // v27: 目标路由——task声明targetChatId(oc_)时路由到对应群topic, 否则回落topics[0]。
        // 结构修复: 此前固定topics[0], 多群Agent的系统轮永远只发第一个群。
        const opts = { senderName: '定时收账' };
        const tc = String(data.targetChatId || data.chat_id || '').trim();
        if (tc) opts.targetId = tc;
        // v23: 不再伪造用户消息(fakeSession 闯门链)——走系统轮一等入口
        setImmediate(() => { handleSystemTurn(msg, opts).catch(e => warn('[TimedRelay] 注入失败:', e.message)); });
    } catch (e) {
        warn('[TimedRelay] 解析结果文件失败:', fp, e.message);
    }
}

// v34: 工具审批飞书卡——监听进程事件, 发批准/拒绝按钮卡, 按钮回调POST回server
let approvalBridgeConfig = null;
// v34.3: requestId → {msgId, decision:'approved'|'rejected'|null} 映射
// decision记录该审批的最终人审结果——重复点击时PATCH回正确终态(而非误显"已失效")
// v34.5: decision落盘持久化(进程重启不丢历史), 文件为插件目录下approvalDecisions.json
const approvalCardMsgIds = new Map();
const APPROVAL_DECISIONS_FILE = require('path').join(__dirname, 'approvalDecisions.json');
function loadApprovalDecisions() {
    try {
        const raw = JSON.parse(require('fs').readFileSync(APPROVAL_DECISIONS_FILE, 'utf8'));
        for (const [rid, info] of Object.entries(raw || {})) {
            if (!approvalCardMsgIds.has(rid)) approvalCardMsgIds.set(rid, { msgId: info.msgId || null, decision: info.decision || null });
        }
    } catch (_) {}
}
function persistApprovalDecision(requestId, info) {
    try {
        let raw = {};
        try { raw = JSON.parse(require('fs').readFileSync(APPROVAL_DECISIONS_FILE, 'utf8')) || {}; } catch (_) {}
        raw[requestId] = { msgId: info.msgId || null, decision: info.decision || null, ts: Date.now() };
        // 只保留最近200条防膨胀
        const keys = Object.keys(raw);
        if (keys.length > 200) for (const k of keys.sort((a, b) => (raw[a].ts || 0) - (raw[b].ts || 0)).slice(0, keys.length - 200)) delete raw[k];
        require('fs').writeFileSync(APPROVAL_DECISIONS_FILE, JSON.stringify(raw));
    } catch (_) {}
}
async function notifyApprovalToFeishu(payload) {
    try {
        const cfg = approvalBridgeConfig || (approvalBridgeConfig = loadBridgeConfig());
        const targetGroup = cfg.approvalTargetId || cfg.ownerOpenId || null;
        if (!targetGroup) { warn('[审批卡] 未配置 approvalTargetId/ownerOpenId, 跳过飞书审批通知'); return; }
        const card = {
            config: { wide_screen_mode: true },
            header: { title: { tag: 'plain_text', content: '🔔 工具调用待审批' }, template: 'orange' },
            elements: [
                { tag: 'div', text: { tag: 'lark_md', content:
                    `**工具**: ${payload.toolName}\n**角色**: ${payload.maid || '-'}\n**参数**: \`${JSON.stringify(payload.args).slice(0, 300)}\`\n**超时**: ${Math.round((payload.approvalTtlMs || 300000) / 60000)} 分钟` } },
                { tag: 'hr' },
                { tag: 'action', actions: [
                    { tag: 'button', text: { tag: 'plain_text', content: '✅ 批准' }, type: 'primary',
                      value: { kind: 'vcp_approval', requestId: payload.requestId, approved: 'true' } },
                    { tag: 'button', text: { tag: 'plain_text', content: '❌ 拒绝' }, type: 'danger',
                      value: { kind: 'vcp_approval', requestId: payload.requestId, approved: 'false' } },
                ] },
            ],
        };
        const sendResult = await sendToolCard(targetGroup, card);
        const sentMsgId = sendResult && sendResult.data && sendResult.data.message_id || null;
        // 登记requestId→{msgId, decision:null}, 供超时PATCH灰卡/重复点击定位卡片
        if (sentMsgId) {
            approvalCardMsgIds.set(payload.requestId, { msgId: sentMsgId, decision: null });
            persistApprovalDecision(payload.requestId, { msgId: sentMsgId, decision: null });
        }
        log('[审批卡] 已发送: ' + payload.toolName + ' requestId=' + payload.requestId + (sentMsgId ? ' msgId=' + sentMsgId : ' (无msgId)'));
    } catch (e) { warn('[审批卡] 发送失败:', e.message); }
}

function bindApprovalBridge() {
    if (bindApprovalBridge._bound) return;
    bindApprovalBridge._bound = true;
    process.on('vcp:approval-request', notifyApprovalToFeishu);
    process.on('vcp:approval-expired', async (payload) => {
        try {
            const entry = approvalCardMsgIds.get(payload.requestId);
            if (!entry || !entry.msgId) return;
            const mid = entry.msgId;
            entry.decision = entry.decision || 'expired';
            persistApprovalDecision(payload.requestId, entry);
            const expiredCard = {
                config: { wide_screen_mode: true },
                header: { title: { tag: 'plain_text', content: '🔔 审批已失效(超时)' }, template: 'grey' },
                elements: [
                    { tag: 'div', text: { tag: 'lark_md', content: `**⏰ 无人审批已超时, 本次工具调用已取消**\n工具: ${payload.toolName}` } },
                ],
            };
            const cfg = approvalBridgeConfig || loadBridgeConfig();
            const token = await tenantAccessToken(cfg);
            const r = await fetchWithTimeout(`https://open.feishu.cn/open-apis/im/v1/messages/${encodeURIComponent(mid)}`, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json; charset=utf-8', Authorization: 'Bearer ' + token },
                body: JSON.stringify({ msg_type: 'interactive', content: JSON.stringify(expiredCard) }),
            }, FEISHU_HTTP_TIMEOUT_MS, '审批卡超时置灰');
            log('[审批卡] 超时置灰 PATCH ' + (r.ok ? 'ok' : 'FAIL ' + r.status));
        } catch (e) { warn('[审批卡] 超时置灰失败:', e.message); }
    });
    log('[审批卡] 事件监听已绑定');
}

async function handleApprovalCardAction(data) {
    let approved = null;
    try {
        const action = data && data.action && data.action.value;
        if (!action || action.kind !== 'vcp_approval') return {};
        approved = String(action.approved) === 'true';
        const port = (approvalBridgeConfig || loadBridgeConfig()).serverPort || 6005;
        const resp = await fetchWithTimeout(`http://127.0.0.1:${port}/v1/tool-approval`, {
            method: 'POST', headers: { 'Content-Type': 'application/json',
                // VCP主服务API需Bearer鉴权——Key由Plugin.js initialize注入(initialConfig.Key)
                Authorization: 'Bearer ' + String(getConfigValue('Key') || '') },
            body: JSON.stringify({ requestId: action.requestId, approved, reason: approved ? '' : '飞书卡片拒绝' }),
        }, FEISHU_HTTP_TIMEOUT_MS, '审批回调');
        const r = await resp.json().catch(() => ({}));
        log('[审批卡] 按钮回调: requestId=' + action.requestId + ' approved=' + approved + ' -> ' + JSON.stringify(r));
        const ok = resp.ok && r && r.ok;
        // v34.4: 记录最终决定; 重复点击(非ok)时查历史决定恢复正确终态
        // v34.5: 内存无记录时从落盘历史加载(进程重启后仍可恢复旧卡终态)
        if (!approvalCardMsgIds.has(action.requestId)) loadApprovalDecisions();
        const known = approvalCardMsgIds.get(action.requestId);
        if (ok && known) {
            known.decision = approved ? 'approved' : 'rejected';
            persistApprovalDecision(action.requestId, known);
        }
        const priorDecision = !ok && known && known.decision;
        // WS模式下回调返回值不会回传飞书——必须主动PATCH卡片更新为终态
        // 未知历史(无落盘记录)用中性文案, 不武断说"已失效"
        const statusText = priorDecision === 'approved' ? '✅ 该审批此前已批准（无需重复操作）'
            : priorDecision === 'rejected' ? '❌ 该审批此前已拒绝（无需重复操作）'
            : !ok && known ? '⚠️ 该审批已失效（超时未审，工具未执行）'
            : !ok ? 'ℹ️ 该审批已被处理或已超时（历史审批，具体结果以对话流为准）'
            : (approved ? '✅ 已批准，命令开始执行' : '❌ 已拒绝，命令未执行');
        try {
            // 实测结构(T41 dump): data.context.open_message_id
            const known0 = approvalCardMsgIds.get(action.requestId);
            const mid = (data.context && (data.context.open_message_id || data.context.message_id))
                || (known0 && known0.msgId)
                || (data.event && data.event.message && (data.event.message.message_id || data.event.message.open_message_id))
                || (data.event && data.event.message_id) || null;
            if (mid) {
                const isApprovedState = ok ? approved : priorDecision === 'approved';
                const headerTitle = priorDecision === 'approved' ? '🔔 审批已批准'
                    : priorDecision === 'rejected' ? '🔔 审批已拒绝'
                    : ok ? (approved ? '🔔 审批已批准' : '🔔 审批已拒绝')
                    : known ? '🔔 审批已失效' : '🔔 审批已处理';
                const headerTpl = (ok || priorDecision) ? (isApprovedState ? 'green' : 'red')
                    : (known ? 'grey' : 'blue');
                const updatedCard = {
                    config: { wide_screen_mode: true },
                    header: { title: { tag: 'plain_text', content: headerTitle }, template: headerTpl },
                    elements: [
                        { tag: 'div', text: { tag: 'lark_md', content: `**${statusText}**\nrequestId: \`${action.requestId}\`` } },
                    ],
                };
                const cfg = approvalBridgeConfig || loadBridgeConfig();
                const token = await tenantAccessToken(cfg);
                const resp2 = await fetchWithTimeout(`https://open.feishu.cn/open-apis/im/v1/messages/${encodeURIComponent(mid)}`, {
                    method: 'PATCH',
                    headers: { 'Content-Type': 'application/json; charset=utf-8', Authorization: 'Bearer ' + token },
                    body: JSON.stringify({ msg_type: 'interactive', content: JSON.stringify(updatedCard) }),
                }, FEISHU_HTTP_TIMEOUT_MS, '审批卡终态更新');
                log('[审批卡] 终态更新 PATCH ' + (resp2.ok ? 'ok' : 'FAIL ' + resp2.status));
                // v34.4: 不删映射——保留decision, 重复点击时恢复正确终态
                if (ok && known) known.msgId = mid;
            } else {
                warn('[审批卡] 回调data缺message_id, 无法PATCH终态');
                try { require('fs').writeFileSync('/tmp/vcp_card_action_dump.json', JSON.stringify(data, null, 2)); } catch (_) {}
            }
        } catch (e2) { warn('[审批卡] 终态更新失败:', e2.message); }
        return { toast: { type: ok ? 'success' : 'warning', content: statusText } };
    } catch (e) { warn('[审批卡] 回调失败:', e.message);
        return { toast: { type: 'error', content: '⚠️ 审批回调失败: ' + e.message } };
    }
}

function startTimedResultsWatcher() {
    try {
        if (!fs.existsSync(TIMED_RESULTS_DIR)) { fs.mkdirSync(TIMED_RESULTS_DIR, { recursive: true }); }
        // 启动时: 只标记存量, 不回放历史(防重启刷屏)
        for (const f of fs.readdirSync(TIMED_RESULTS_DIR)) {
            if (f.endsWith('.json')) deliveredTimedResults.add(f);
        }
        timedResultsWatcher = fs.watch(TIMED_RESULTS_DIR, (evt, filename) => {
            if (evt !== 'change' && evt !== 'rename') return;
            if (!filename || !String(filename).endsWith('.json')) return;
            const fp = path.join(TIMED_RESULTS_DIR, filename);
            // 写入可能未完成, 延迟读
            setTimeout(() => { if (fs.existsSync(fp)) deliverTimedResultFile(fp); }, 800);
        });
        log('[TimedRelay] 定时结果回送 watcher 已启动:', TIMED_RESULTS_DIR);
    } catch (e) {
        warn('[TimedRelay] watcher 启动失败(定时回送降级为不可用):', e.message);
    }
}

function startInflightWatchdog() {
    if (watchdogTimer) return;
    watchdogTimer = setInterval(inflightWatchdogTick, 60 * 1000);
}

function markInflight(session) {
    inflight.set(session.targetId, {
        targetId: session.targetId, messageId: session.messageId,
        text: String(session.text || ''), since: Date.now(), reported: false,
    });
    startInflightWatchdog();
}

function clearInflight(targetId) { inflight.delete(targetId); }

// 启动自检：进程重启会杀在途回复——把悬空消息显式报告出来
function reportOrphanedInflight() {
    if (inflight.size === 0) return;
    for (const [chatId, st] of inflight) {
        if (Date.now() - st.since < 30 * 1000) continue; // 刚登记的可能是新消息，跳过
        sendFeishuText(st.targetId,
            `⚠️ [状态反馈] 服务在处理你这条消息（"${st.text.slice(0, 30)}…"）期间重启了，回复已丢失。\n` +
            `请重发一次。`,
            { replyToMessageId: st.messageId }).catch(() => {});
    }
    inflight.clear();
}

async function readStreamResponse(response) {
    const reader = response.body?.getReader();
    if (!reader) return '';
    const decoder = new TextDecoder('utf8');
    let buffer = '';
    let content = '';
    let __v26WatchPos = 0; // v26: 看门狗游标

    while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split(/\r?\n/);
        buffer = lines.pop() || '';
        for (const line of lines) {
            const trimmed = line.trim();
            if (!trimmed.startsWith('data:')) continue;
            const payload = trimmed.slice(5).trim();
            if (!payload || payload === '[DONE]') continue;
            try {
                const json = JSON.parse(payload);
                content += json.choices?.[0]?.delta?.content
                    || json.choices?.[0]?.message?.content
                    || json.choices?.[0]?.text
                    || '';
            } catch (_) {
                content += payload;
            }
            // v26: 退行看门狗——每积4K字符查尾窗, 命中即 cancel(级联断服务端循环, 断10分钟长烧)+存根返回
            if (content.length - __v26WatchPos >= 4000) {
                __v26WatchPos = content.length;
                const dw = degenerateTailAnalysis(content);
                if (dw.hit) {
                    warn('[v26熔断] 流内退行命中 ' + dw.rule + ' len=' + content.length + ' sample=' + JSON.stringify(String(dw.sample).slice(0, 40)));
                    try { await reader.cancel(); } catch (_) {}
                    return buildDegenerateStub(content, true);
                }
            }
        }
    }
    return content;
}

// 续踢（09-16）：在原历史后追加一条 user 消息再调一轮，专用于截断续写
// ---- v10: 流式缓存 + 工具调用即时成卡 ----
// 边收流边检测新完成的 [[VCP调用结果信息汇总...VCP调用结果结束]] 块，
// 每检测到一个立刻 emit(text) 单独发卡（feishuFormatReply 格式化后的 ✅/❌ 行），
// 收尾返回 { content, sentToolLines } —— content 是全量文本（供历史/续踢用），
// sentToolLines 是已发过的工具行数组（收尾消息里剔除，避免重复）。
const VCP_RESULT_BLOCK_RE = /\[\[VCP调用结果信息汇总:([\s\S]*?)VCP调用结果结束\]\]/g;

async function readStreamResponseIncremental(response, emit, topicSession = null) { // v32.2: +topicSession(即时interactive卡投递目标)
    const reader = response.body?.getReader();
    if (!reader) return { content: '', emittedBlocks: [] };
    const decoder = new TextDecoder('utf8');
    let buffer = '';
    let content = '';
    let __v26WatchPos = 0; // v26: 看门狗游标
    let lastScanPos = 0;          // content 里已扫描过工具块的位置
    let lastEmitLen = 0;          // 上次 emit 时 content 的长度
    const emittedBlocks = [];     // v10.1: 已发卡的原文块（结果块+配对请求块），收尾整块剥离
    let emitSeq = 0;

    const flushNewToolBlocks = async () => {
        if (typeof emit !== 'function') return;
        // 只在"新到达的文本"里找完整工具结果块；已扫过的区域跳过
        const searchFrom = Math.max(0, lastScanPos - 40); // 回看一点防块边界跨 chunk
        const seg = content.slice(searchFrom);
        VCP_RESULT_BLOCK_RE.lastIndex = 0;
        let m;
        const found = [];
        while ((m = VCP_RESULT_BLOCK_RE.exec(seg)) !== null) {
            found.push({ block: m[0], start: searchFrom + m.index, end: searchFrom + m.index + m[0].length });
        }
        if (!found.length) { lastScanPos = content.length; return; }
        cardDebug(`扫描命中 ${found.length} 个结果块 (content已积${content.length}字符)`);
        for (const f of found) {
            // v10.1: 向前找最近的完整 TOOL_REQUEST 块一起格式化 → 卡= 🔧请求行+✅/❌结果行(带参数)
            const before = content.slice(0, f.start);
            const REQ_RE = /<<<\[TOOL_REQUEST\]>>>[\s\S]*?<<<\[END_TOOL_REQUEST\]>>>/g;
            let lastReq = null, rm;
            while ((rm = REQ_RE.exec(before)) !== null) lastReq = rm[0];
            const fragment = (lastReq ? lastReq + '\n' : '') + f.block;
            const card = feishuFormatReply(fragment).trim();
            if (card) {
                emitSeq++;
                // v32: 即时工具卡升级为 interactive collapsible_panel(真折叠);
                // 卡片构造失败或发送异常时回退旧⚡文本卡, 保证工具活动永不失明
                emittedBlocks.push(f.block);
                if (lastReq) emittedBlocks.push(lastReq);
                const facts = (feishuFormatReply.cardFacts || [])[0];
                let sentCard = false;
                if (facts) {
                    try {
                        const ic = buildToolCard({ tool: facts.tool, argsText: facts.argsText, ok: facts.ok, hint: facts.hint, detail: facts.detail });
                        cardDebug(`发卡 #${emitSeq}(interactive): ${facts.tool} ok=${facts.ok}`);
                        // v32.2: topic.targetId常为null, 真值在_metadata.chatId(实测topics配置)
                        // v32.2终: topicSession是包装对象{topic,session,...}, 投递目标在session.targetId
                        const __cardTarget = topicSession?.session?.targetId || topicSession?.topic?._metadata?.chatId || (topicSession?._metadata || {}).chatId;
                        await sendToolCard(__cardTarget, ic);
                        sentCard = true;
                    } catch (e) { warn('interactive卡失败, 回退⚡文本卡:', e.message); }
                }
                if (!sentCard) {
                    cardDebug(`发卡 #${emitSeq}: ${card.slice(0, 60)}`);
                    try { await emit(`⚡ ${card}`, true); }
                    catch (e) { warn('即时工具卡发送失败:', e.message); }
                }
            }
            lastScanPos = Math.max(lastScanPos, f.end);
        }
        lastEmitLen = content.length;
    };

    while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split(/\r?\n/);
        buffer = lines.pop() || '';
        for (const line of lines) {
            const trimmed = line.trim();
            if (!trimmed.startsWith('data:')) continue;
            const payload = trimmed.slice(5).trim();
            if (!payload || payload === '[DONE]') continue;
            try {
                const json = JSON.parse(payload);
                content += json.choices?.[0]?.delta?.content
                    || json.choices?.[0]?.message?.content
                    || json.choices?.[0]?.text
                    || '';
            } catch (_) {
                content += payload;
            }
            // v26: 退行看门狗——每积4K字符查尾窗, 命中即 cancel(级联断服务端循环, 断10分钟长烧)+存根返回
            if (content.length - __v26WatchPos >= 4000) {
                __v26WatchPos = content.length;
                const dw = degenerateTailAnalysis(content);
                if (dw.hit) {
                    warn('[v26熔断] 流内退行命中 ' + dw.rule + ' len=' + content.length + ' sample=' + JSON.stringify(String(dw.sample).slice(0, 40)));
                    try { await reader.cancel(); } catch (_) {}
                    return { content: buildDegenerateStub(content, true), emittedBlocks };
                }
            }
        }
        await flushNewToolBlocks();
    }
    // 流结束后再扫一次（兜底：最后一块可能在收尾 chunk 里）
    await flushNewToolBlocks();
    // v10.11: 双重封装剥离——上游链可能把原始 SSE 包进 delta.content，终点兜底
    content = stripSseWrapper(content);
    return { content, emittedBlocks };
}

async function callVcpAgentWithExtra(topicSession, extraUserText, emit = null) { // v20: +emit
    const settings = loadSettings();
    if (!settings.vcpApiKey) throw new Error('settings.json 缺少 vcpApiKey');
    const messages = buildMessagesForVcp(topicSession);
    // 原样保留上一轮 assistant 回复（截断版）+ 追加续写指令
    messages.push({ role: 'assistant', content: topicSession.lastRawReply || '' });
    messages.push({ role: 'user', content: extraUserText });
    const modelConfig = modelConfigFromAgent(topicSession.agentConfig);
    const messageId = `msg_feishu_${Date.now()}_${randomSuffix()}`;
    const body = {
        messages: stripInternalMessageFields(messages),
        ...modelConfig,
        requestId: messageId,
    };
    const response = await (VCP_LOOP_DISPATCHER ? require('undici').fetch : fetch)(
        vcpUrlFromSettings(settings), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${settings.vcpApiKey}` },
            body: JSON.stringify(body),
            dispatcher: VCP_LOOP_DISPATCHER,
            signal: AbortSignal.timeout(7200_000), // v10.4 灾难兜底非任务定时器: 正常任务永不触发
        });
    if (!response.ok) throw new Error(`${response.status} - VCP 请求失败`);
    // v10.11 (09-18 02:40): 续踢响应可能是 SSE 流（modelConfig 带 stream:true 时 VCP 走流式工具循环）。
    // 根因修复：此前 response.text()+JSON.parse 失败后直接 return text，把 80 万字符原始 SSE
    // 当续写内容返回 → 群里 295 段刷屏。现在：SSE 响应用 readStreamResponse 正确解析 delta.content；
    // JSON 解析失败也先 stripSseWrapper 抢救，剥不出有效内容才原样返回（上游错误页等）。
    const kickContentType = response.headers.get('content-type') || '';
    if (kickContentType.includes('text/event-stream')) {
        // v20: kick path also streams incremental tool cards (no more mega-card)
        if (typeof emit === 'function') {
            const r = await readStreamResponseIncremental(response, emit, topicSession);
            return { content: stripSseWrapper(replyLengthFuse(r.content)), emittedBlocks: r.emittedBlocks };
        }
        const streamed = await readStreamResponse(response);
        const cleaned = stripSseWrapper(streamed || '');
    if (!cleaned.trim()) warn('续踢收到 SSE 流但解析出 0 字符正文');
        return cleaned;
    }
    const text = await response.text();
    let data = {};
    try { data = text ? JSON.parse(text) : {}; } catch (_) {
        const salvaged = stripSseWrapper(text).trim();
        if (salvaged) {
            warn(`续踢响应 JSON 解析失败，SSE 剥壳抢救出 ${salvaged.length} 字符`);
            return salvaged;
        }
        return text;
    }
    return data?.choices?.[0]?.message?.content || data?.choices?.[0]?.text || data?.content || '';
}

// v10.9 [CardDebug] 逐轮诊断: 每次调用记一行——走哪条路径、读到什么、发了几张卡
function cardDebug(...args) {
    console.log('[CardDebug]', ...args);
}

// v10.11 (09-18 02:30): SSE 双重封装剥离——上游链(new-api/智谱)曾把原始 SSE 流文本
// 包进 delta.content 回吐(实证: 800196字符回复, 97%为 data: {...} 行, 含 VCP 自家哨兵
// chatcmpl-vcp-start 与 reasoning_content 字段名)。任何情况下正文都不该长这样，
// 终点防御：识别连续 SSE 块并整体剥离，保留块外真实正文。
function stripSseWrapper(text) {
    if (typeof text !== 'string') return text;
    // 快速门槛：没特征直接返回（零开销路径）
    if (!text.includes('data: {') && !text.includes('data:{')) return text;
    const isChunkLine = (t) => {
        if (!t.startsWith('data:')) return false;
        const payload = t.slice(5).trim();
        if (!payload) return false;
        if (payload === '[DONE]') return true;
        try {
            const j = JSON.parse(payload);
            return !!(j && typeof j === 'object' &&
                (j.object === 'chat.completion.chunk' || String(j.id || '').startsWith('chatcmpl-') || Array.isArray(j.choices)));
        } catch (_) { return false; }
    };
    const lines = text.split('\n');
    // 只有确实存在 chunk 行才启用剥离（防止把正常引用 data: 的散文误剥）
    if (!lines.some(l => isChunkLine(l.trim()))) return text;
    const before = text.length;
    const kept = lines.filter(l => !isChunkLine(l.trim()));
    const stripped = kept.join('\n');
    if (stripped.length !== before) {
        console.log(`[SSEGuard] 剥离双重封装 SSE 文本: ${before} → ${stripped.length} 字符`);
    }
    return stripped;
}

// v10.11: 单条回复硬顶保险丝——剥离后仍超长的异常回复（>15万字符）截断保群，
// 防止 295 段刷屏重演。截断时附诊断尾注。
function replyLengthFuse(text) {
    const FUSE = 150_000;
    if (typeof text === 'string' && text.length > FUSE) {
        console.log(`[SSEGuard] 保险丝熔断: 回复 ${text.length} 字符 > ${FUSE}，截断`);
        return text.slice(0, FUSE) + `\n\n[保险丝] 回复异常超长(${text.length}字符)已截断，完整版存服务器日志。疑似上游流泄漏。`;
    }
    return text;
}

async function callVcpAgent(topicSession, emit = null) {
    const __dbgStreamCfg = topicSession?.agentConfig?.streamOutput;
    cardDebug(`callVcpAgent enter: streamOutput=${__dbgStreamCfg} (${typeof __dbgStreamCfg}) emit=${typeof emit === 'function'} topic=${topicSession?.agentId || '?'}`);
    const settings = loadSettings();
    if (!settings.vcpApiKey) throw new Error('settings.json 缺少 vcpApiKey');
    const messages = buildMessagesForVcp(topicSession);
    const modelConfig = modelConfigFromAgent(topicSession.agentConfig);
    const messageId = `msg_feishu_${Date.now()}_${randomSuffix()}`;
    const vcpchatExtensions = buildVcpChatExtensions(messages);
    const body = {
        messages: stripInternalMessageFields(messages),
        ...modelConfig,
        requestId: messageId,
    };
    if (vcpchatExtensions) body.vcpchatExtensions = vcpchatExtensions;

    const fetchImpl = VCP_LOOP_DISPATCHER
        ? require('undici').fetch
        : fetch;
    const response = await fetchImpl(vcpUrlFromSettings(settings), {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${settings.vcpApiKey}`,
        },
        body: JSON.stringify(body),
        dispatcher: VCP_LOOP_DISPATCHER,
        signal: AbortSignal.timeout(7200_000), // v10.4 灾难兜底非任务定时器: 正常任务永不触发
    });

    if (!response.ok) {
        const text = await response.text();
        throw new Error(`${response.status} - ${text || 'VCP 请求失败'}`);
    }

    const contentType = response.headers.get('content-type') || '';
    cardDebug(`VCP响应: status=${response.status} content-type=${contentType} modelConfig.stream=${modelConfig.stream} emit=${typeof emit === 'function'}`);
    if (modelConfig.stream === true && !contentType.includes('application/json')) {
        // v10: 增量模式——边收边发工具卡；无 emit 时退回旧全量行为
        if (typeof emit === 'function') {
            const r = await readStreamResponseIncremental(response, emit, topicSession);
            // v10.11: 终点防御——双重封装 SSE 剥离 + 超长保险丝（09-18 800K字符295段刷屏事故）
            r.content = replyLengthFuse(stripSseWrapper(r.content));
            cardDebug(`增量读取完成: content=${r.content.length}字符 结果块=${r.emittedBlocks.filter(b => b.includes('调用结果')).length} 请块=${r.emittedBlocks.filter(b => b.includes('TOOL_REQUEST')).length}`);
            return r;
        }
        const streamed = await readStreamResponse(response);
        if (streamed) return streamed;
        return '';
    }

    const text = await response.text();
    let data = {};
    try {
        data = text ? JSON.parse(text) : {};
    } catch (_) {
        return text;
    }
    return data?.choices?.[0]?.message?.content
        || data?.choices?.[0]?.text
        || data?.content
        || '';
}

function pruneProcessedMessages(now = Date.now()) {
    for (const [messageId, seenAt] of processedMessageIds) {
        if (now - seenAt > MESSAGE_DEDUPE_TTL_MS) processedMessageIds.delete(messageId);
    }
}

function isDuplicateMessage(messageId) {
    if (!messageId) return false;
    const now = Date.now();
    pruneProcessedMessages(now);
    if (processedMessageIds.has(messageId)) return true;
    processedMessageIds.set(messageId, now);
    return false;
}

function isUserAllowed(senderId, bridgeConfig) {
    return bridgeConfig.allowedUsers.length === 0 || bridgeConfig.allowedUsers.includes(senderId);
}

function isGroupMentioned(message) {
    const mentions = (message && message.mentions) || []; // v22: 防御 undefined message
    if (!Array.isArray(mentions) || mentions.length === 0) return false;
    return mentions.some(item => item.tenant_key || item.name === config.botName);
}

// v5 (09-15): 飞书 post 富文本(引用回复/带格式消息)→纯文本。此前被 type!=='text' 静默丢弃,
// 群里引用回复 @机器人 时 bot 完全无反应(2026-09-15 01:25 实锤)。at 元素(提及)跳过只留正文。
function extractPostText(content) {
    const rich = content?.zh_cn || content;
    const parts = [];
    if (rich?.title) parts.push(String(rich.title));
    if (Array.isArray(rich?.content)) {
        for (const paragraph of rich.content) {
            const line = (Array.isArray(paragraph) ? paragraph : [paragraph])
                .filter(el => (el?.tag === 'text' || el?.tag === 'a') && typeof el.text === 'string' && el.text)
                .map(el => el.text)
                .join('')
                .trim();
            if (line) parts.push(line);
        }
    }
    return parts.join('\n');
}

function parseFeishuEvent(rawEvent) {
    const event = rawEvent.event || rawEvent;
    const message = event.message || event;
    const sender = event.sender || message.sender || {};
    const senderId = sender.sender_id?.open_id || sender.sender_id?.user_id || sender.sender_id?.union_id || '';
    const chatType = message.chat_type || event.chat_type || 'p2p';
    const chatId = chatType === 'group' ? message.chat_id : senderId;
    const targetId = chatId || senderId;
    const receiveIdType = chatType === 'group' ? 'chat_id' : inferReceiveIdType(targetId);
    let text = '';
    const msgType = message.msg_type || message.message_type;

    try {
        const content = typeof message.content === 'string' ? JSON.parse(message.content) : message.content;
        text = content?.text || '';
        if (!text && msgType === 'post') text = extractPostText(content);
    } catch (_) {
        text = String(message.content || '');
    }

    return {
        message,
        type: msgType,
        messageId: message.message_id || event.message_id || '',
        senderId,
        senderName: sender.sender_id?.union_id || senderId || '飞书用户',
        chatType,
        chatId,
        targetId,
        receiveIdType,
        sessionKey: sessionKeyFor(chatId, senderId),
        text: stripMentionPrefix(text),
    };
}

async function handleFeishuEvent(rawEvent) {
    const bridgeConfig = loadBridgeConfig();
    // v21/v23: 已解析 session(_parsed)或系统轮(_system)不过 parse
    const session = rawEvent && (rawEvent._parsed || rawEvent._system) ? rawEvent : parseFeishuEvent(rawEvent);

    if (session.type !== 'text' && session.type !== 'post') {
        debug('跳过非文本消息:', session.type);
        return;
    }
    // v23: 门链只辖真实飞书事件——系统轮(_system, handleSystemTurn 入口)不是用户消息,
    // 跳过去重/@提及/白名单全部门; 信任边界在构造侧(本机进程), 不在消息侧。
    // v21 教训: 伪装用户消息逐门闯关, 漏一道门(undefined.mentions)整链静默死。
    if (!rawEvent?._system) {
        if (isDuplicateMessage(session.messageId)) {
            debug('跳过重复消息:', session.messageId);
            return;
        }
        if (session.chatType === 'group' && !isGroupMentioned(session.message)) {
            debug('群聊消息未 @ 机器人，跳过');
            return;
        }
        if (!session.senderId) {
            warn('消息缺少 sender_id');
            return;
        }
        if (!isUserAllowed(session.senderId, bridgeConfig)) {
            log('用户不在白名单中:', session.senderId);
            return;
        }
    }
    if (!session.text) {
        debug('消息内容为空');
        return;
    }

    stats.messagesReceived++;
    stats.lastMessageAt = new Date().toISOString();
    log(`收到${rawEvent?._system ? '[系统轮] ' : ''}消息: from=${session.senderId} chat_type=${session.chatType} text=${session.text.slice(0, 80)}`);
    markInflight(session); // v10.2 状态监听

    const agent = findAgent(bridgeConfig.bindAgent);
    if (!agent) throw new Error(`未找到绑定 Agent: ${bridgeConfig.bindAgent}`);

    const topicSession = ensureFeishuTopic(agent, session);
    appendHistory(topicSession.historyPath, buildUserMessage(session.text, session));

    if (bridgeConfig.streamReply) {
        try {
            await sendFeishuText(session.targetId, bridgeConfig.streamHint, { replyToMessageId: session.messageId });
        } catch (err) {
            warn('发送提示语失败:', err.message);
        }
    }

    try {
        // v10: 流式缓存——stream 开启时每个工具结果块一到就单独发卡
        const agentStream = toBoolean(topicSession.agentConfig?.streamOutput, false);
        let sentToolLines = [];
        const emitToolCard = async (text, forcePost = false) => {
            await sendFeishuText(session.targetId, text, { replyToMessageId: session.messageId, forcePost });
        };
        let reply = agentStream
            ? await callVcpAgent(topicSession, emitToolCard)
            : await callVcpAgent(topicSession);
        if (reply && typeof reply === 'object' && !Array.isArray(reply) && typeof reply.content === 'string') {
            sentToolLines = reply.emittedBlocks || []; reply = reply.content;
        }
        if (!reply) throw new Error('VCP 后端未返回有效回复');
        // v26: 终稿退行守卫——非流路径/漏网的完整回复在此存根化(历史不进毒few-shot)
        if (typeof reply === 'string' && isDegenerateText(reply)) {
            warn('[v26熔断] 终稿退行指纹命中 len=' + reply.length + ' — 存根化, 不回灌历史');
            reply = buildDegenerateStub(reply);
        }
        // 截断续踢（09-16）：glm-5.3 中途弃笔检测——回复无句读收尾（。：？！\n）且无工具块时，
        // 视为弃笔，自动注入续踢提示再调一次，把断句接完。最多 1 次，防循环。
        const looksTruncated = (t) => {
            if (typeof t !== 'string') return false;
            const s = t.trim();
            // v24 (09-20): 只有"尾部存在未闭合工具块"才是活工具轮(续踢交给 VCP loop)。
            // "任意位置含 TOOL_REQUEST"不算——10:37 实案: 工具块早已执行完, 尾部散文断在
            // "落盘："(冒号弃笔), 旧判定把已执行完的历史块当活工具轮, 续踢被跳过, 断头直投。
            // 注: 裸 'TOOL_REQUEST' 是 END 标记的子串会污染判定; 用完整括号标记计数配对
            // (<<<[TOOL_REQUEST]>>> 不是 <<<[END_TOOL_REQUEST]>>> 的子串, 无污染)
            // v28: 扫描器计数——反引号包裹的字面示例不算开块(朴素split会误判活工具轮)
            let opens = 0, cursor = 0;
            while (cursor < s.length) {
                const si = s.indexOf('<<<[TOOL_REQUEST]>>>', cursor);
                if (si === -1) break;
                if (!vcpIsBacktickWrapped(s, si, VCPTOOL_START)) opens++;
                cursor = si + VCPTOOL_START.length;
            }
            let closes = 0; cursor = 0;
            while (cursor < s.length) {
                const si = s.indexOf('<<<[END_TOOL_REQUEST]>>>', cursor);
                if (si === -1) break;
                if (!vcpIsBacktickWrapped(s, si, VCPTOOL_END)) closes++;
                cursor = si + VCPTOOL_END.length;
            }
            if (opens > closes) return false; // 真活工具轮(有未闭合块), VCP loop 会接
            // 09-17 21:25 样本：14 字断在冒号（"…第二回合现场："）后直接 EOS，承诺未交付。
            // 冒号收尾=弃笔信号，不论长度；旧版 40 字门槛+冒号算合法句读，双双漏放。
            // v29 (10-05): 撤掉 length>=8 门槛——T10/T11 五连复现"块前置："(4字符)冒号截停,
            // 两边门槛(40字/8字)全漏放。冒号收尾一律续踢, 无长度豁免。
            if (/[：:]\s*$/.test(s)) return true;
            if (s.length < 40) return false;
            // v28.2 (10-05): 收尾白名单扩充——T5实锤"读到=[GOLDEN-VALUE-9173]"以]收尾被误判截断,
            // 续踢注入接缝(日志:截断续踢误触发首例)。合法收尾补充: ] ) 英文.!? 省略号 引号" ' 》 加粗** 围栏`
            return /[。；！？\n\]\)\.\!?…"\u2019》*`]\s*$/.test(s) === false; // 注: 冒号已上移单独判
        };
        if (looksTruncated(reply)) {
            log(`检测到疑似截断回复（长度 ${reply.length}，无句读收尾），自动续踢一次`);
topicSession.lastRawReply = stripThoughtChains(stripOrphanBlocks(normalizeProtocolMarkers(reply))); // v16: 续踢路径喂回的也是清洗版
            topicSession.pendingKick = (topicSession.pendingKick || 0) + 1;
            if (topicSession.pendingKick <= 1) {
                try {
                    let kick = await callVcpAgentWithExtra(topicSession,
                        `你上一条回复疑似中途截断（最后 30 字：${JSON.stringify(reply.slice(-30))}）。请从断点原样续写完成，不要重复已写内容，不要道歉，直接续。`, emitToolCard);
                    if (kick && typeof kick === 'object' && typeof kick.content === 'string') { if (Array.isArray(kick.emittedBlocks)) sentToolLines.push(...kick.emittedBlocks); kick = kick.content; }
                    if (kick && kick.length > 20) reply = reply + '\n' + kick;
                } catch (e) {
                    warn('续踢失败（保留原截断回复）:', e.message);
                }
            }
        } else {
            topicSession.pendingKick = 0;
        }
        // v29 (10-05): 极短正文+零工具活动复询——T10/T11 实锤 4 字符"块前置："型截停,
        // 无句读信号(非冒号收尾也防漏), 正文<12字符且本轮无任何工具卡时视为弃笔, 续踢一次要求重写。
        const tinyBody = reply.replace(/[\\s\\S]*?<<<[END_TOOL_REQUEST]>>>/g, '').trim(); // 剥掉工具块后的纯正文
        if (tinyBody.length > 0 && tinyBody.length < 12 && sentToolLines.length === 0 && !looksTruncated(reply)) {
            log(`检测到极短回复（正文 ${tinyBody.length} 字符, 零工具活动）, 续踢要求重写完整回复`);
            topicSession.pendingKick = (topicSession.pendingKick || 0) + 1;
            if (topicSession.pendingKick <= 1) {
                try {
                    let rewrite = await callVcpAgentWithExtra(topicSession,
                        `你上一条回复只有几个字（${JSON.stringify(reply.slice(0, 40))}）, 明显是中途弃笔。请重新给出完整回复, 直接写正文, 不要重复道歉。`,
                        emitToolCard);
                    if (rewrite && typeof rewrite === 'object' && typeof rewrite.content === 'string') {
                        if (Array.isArray(rewrite.emittedBlocks)) sentToolLines.push(...rewrite.emittedBlocks);
                        rewrite = rewrite.content;
                    }
                    if (rewrite && rewrite.trim().length > tinyBody.length) reply = rewrite;
                } catch (e) {
                    warn('极短回复续踢失败（保留原回复）:', e.message);
                }
            }
        }
        // 显示层格式化：飞书发摘要版，历史文件存原始版（Agent 上下文不丢）
        // v10.1: 已发卡的原文块从 reply 整块剥离后再格式化（收尾=纯正文，工具活动全在卡里）
        if (sentToolLines.length) {
            for (const block of sentToolLines) reply = String(reply).replace(block, '');
        }
        let display = feishuFormatReply(reply);
        // v10.2: 假宣告检测——正文宣告了动作但全程无工具活动 → 追加警示行
        if (looksLikeAnnounceWithoutTool(reply, sentToolLines.length > 0)) {
            // v19: 假宣告从警示升格为踢——宣告动作但零工具调用 = turn 自杀, 14字短宣告曾放行(17:32 死因)
            warn('[监听] 假宣告踢: reply 尾部=', JSON.stringify(String(reply).slice(-60)));
            topicSession.pendingKick = (topicSession.pendingKick || 0) + 1;
            if (topicSession.pendingKick <= 1) {
                try {
                    let kick = await callVcpAgentWithExtra(topicSession,
                        '你刚才宣告了工具动作但没有发出任何工具调用（宣布不调=假宣告）。请立即用 <<<[TOOL_REQUEST]>>> 块实际调用你宣告的工具执行该动作。不要重复说明，直接调用。', emitToolCard);
                    if (kick && typeof kick === 'object' && typeof kick.content === 'string') { if (Array.isArray(kick.emittedBlocks)) sentToolLines.push(...kick.emittedBlocks); kick = kick.content; }
                    if (kick && /TOOL_REQUEST/.test(kick)) {
                        reply = reply + '\n' + kick;
                        warn('[监听] 假宣告踢命中: kick 含工具块, 长度', String(kick).length);
                    } else {
                        warn('[监听] 假宣告踢未命中(回包无工具块), 保留原回复');
                    }
                } catch (e) {
                    warn('假宣告踢失败(保留原回复):', e.message);
                }
            }
            display = feishuFormatReply(reply);
            display += '\n⚠️ [监听] 以上回复曾宣告工具动作但未实际调用，已自动要求补发调用。';
        }
        // v21: 正文与工具行拆分——工具活动行(⚡/🔧/✅/❌/📋/⚠️[监听])抽成独立消息, 正文纯散文
        const splitDisplayLines = (display || '').split('\n');
        const proseLines = [];
        const toolLineMsgs = [];
        let toolBuf = [];
        const flushToolBuf = () => { if (toolBuf.length) { toolLineMsgs.push(toolBuf.join('\n')); toolBuf = []; } };
        for (const ln of splitDisplayLines) {
            if (/^\s*(?:⚡|🔧|✅|❌|📋|⚠️ \[监听\])/.test(ln)) { toolBuf.push(ln.replace(/^\s+/, '')); }
            else { flushToolBuf(); proseLines.push(ln); }
        }
        flushToolBuf();
        const proseOnly = proseLines.join('\n').replace(/\n{3,}/g, '\n\n').trim();
        if (proseOnly) {
            // v32: mermaid围栏先离线渲染成PNG(image消息), 正文剥离围栏再投
            const proseClean = await drainMermaidBlocks(session.targetId, proseOnly, { replyToMessageId: session.messageId });
            if (proseClean) {
                await sendFeishuText(session.targetId, proseClean, { replyToMessageId: session.messageId });
            }
        }
        for (let ti = 0; ti < toolLineMsgs.length; ti++) {
            await sendFeishuText(session.targetId, toolLineMsgs[ti], { replyToMessageId: session.messageId });
        }
        // v15: 历史存归一化版——畸形标记归正后入历史, 断回流污染环(内容不丢, 标记形态归标准)
        // v16: 历史存清洗版——归一化+孤儿剥除+思维链剥除(裸存孤儿/思维链=漂移温床+token黑洞)
        // v31 (10-05): 异常回复不进history——续踢后正文仍<16字符且零工具活动=弃笔终态。
        // VCPChat用户在环会把坏回复编辑掉(messageContextMenu EditMode); 无人值守SystemTurn的
        // 等价物=不写入。T10/T11/T13实锤: 坏回复进历史后被模型逐字模仿(块前置:/占位文案), 形成污染环。
        {
            const __prose = String(reply).replace(/[\s\S]*?<<<[END_TOOL_REQUEST]>>>/g, '').replace(/<<<\[(?:END_)?ROLE_DIVIDE_\w+\]>>>/g, '').trim();
            const __degenerate = __prose.length > 0 && __prose.length < 16 && sentToolLines.length === 0;
            if (__degenerate) {
                warn(`[v31] 弃笔终态(正文${__prose.length}字符, 零工具) — 不入history不投递, 发运维告知: ${JSON.stringify(__prose.slice(0, 40))}`);
                // 弃笔回复不进群: 用户看到9字符烂消息无行动价值。发运维告知行代替(带上下文可追溯)。
                // v31.1: 回滚本轮user回执——孤儿user(有问无答)也是模仿诱因, 删对保持role交替
                try {
                    const __h = readJson(topicSession.historyPath, []);
                    if (__h.length && __h[__h.length - 1] && __h[__h.length - 1].role === 'user') {
                        __h.pop(); writeJson(topicSession.historyPath, __h);
                        warn('[v31.1] 已回滚本轮user回执, history保持role交替');
                    }
                } catch (e) { warn('[v31.1] user回执回滚失败:', e.message); }
                try {
                    await sendFeishuText(session.targetId, `⚠️ [运维] 本轮回复异常截停（${__prose.length}字），已拦截不入档。系统将随下轮消息自动恢复。`, { replyToMessageId: session.messageId });
                } catch (e) { warn('[v31] 运维告知发送失败:', e.message); }
            } else {
                appendHistory(topicSession.historyPath, buildAssistantMessage(stripThoughtChains(stripOrphanBlocks(normalizeProtocolMarkers(reply))), agent.id, topicSession.agentConfig, session));
            }
        }
        stats.messagesProcessed++;
        clearInflight(session.targetId); // v10.2 状态监听
        log(`回复已发送: target=${session.targetId} topic=${topicSession.topic.id} length=${reply.length} 投递版=${(display || '').length}`);
    } catch (err) {
        stats.messagesFailed++;
        setLastError(err);
        warn('处理消息失败:', err.message);
        try {
            await sendFeishuText(session.targetId, `抱歉，处理出错：${err.message}`, { replyToMessageId: session.messageId });
        } catch (_) {}
        clearInflight(session.targetId); // v10.2 出错也算闭环
    }
}

// v10.2: 宣布不调检测——回复以宣告动词结尾但既无工具块也无工具结果 = 假宣告（"现在发送飞书并落档:"式）
function looksLikeAnnounceWithoutTool(reply, hadToolActivity = false) {
    if (!reply || reply.length < 8) return false; // v19: 30→8, 14字宣告句曾漏网(09-19 17:32 死因)
    const t = String(reply).trimEnd();
    // v32.2: hadToolActivity(本轮已发工具卡)时只拦悬挂冒号形态——
    // 工具已真实执行, 正文残留的宣告短语(如"执行①后一并作答")是叙述不是空头支票。
    // T17误报实锤: emittedBlocks剥离后的正文含"块前置：执行①..."被当假宣告踢。
    if (hadToolActivity) {
        const tailZ = t.slice(-120);
        return /(现在|接下来|马上|立即|先)?(发送|调用|执行|写入|查|取|发射|开跑|重启|查账|对照|复刻|落)[^。；\n]{0,30}[:：]\s*$/.test(tailZ);
    }
    // v20: 悬挂宣告(09-19 23:11 实案)——工具块存在但最后一块之后的散文仍以"宣告动词+冒号"收尾 = 承诺未交付
    const lastToolEnd = Math.max(t.lastIndexOf('VCP调用结果结束'), t.lastIndexOf('<<<[END_TOOL_REQUEST]>>>'));
    const zone = lastToolEnd >= 0 ? t.slice(lastToolEnd) : t;
    const tail = zone.trimEnd().slice(-120);
    if (lastToolEnd >= 0) {
        // 有工具活动: 只拦冒号收尾的悬挂形态("先查白名单:"), 句号收尾可能是正常总结不踢
        return /(现在|接下来|马上|立即|先)?(发送|调用|执行|写入|查|取|发射|开跑|重启|查账|对照|复刻|落)[^。；\n]{0,30}[:：]\s*$/.test(tail);
    }
    // 结尾 120 字符内出现宣告动词短语
    // v32.2: 句号分支删除——"执行→收账闭环, 终验通过。"(T17)这种完成时陈述被当宣告误踢。
    // 宣告=承诺未交付, 中文形态是冒号/悬垂; 句号=已陈述。完成时词再豁免一道。
    const tail0 = t.slice(-120);
    const announces = /(现在|接下来|马上|立即)?(发送|发送飞书|调用|执行|写入|落档|保存|通知|提交|取|发射|开跑|重启|查账|对照|复刻)[^。；\n]{0,30}[:：]\s*$|(现在|接下来|马上).{0,12}(发送|调用|执行|落档)/;
    if (announces.test(tail0)) {
        const doneWords = /(已完成|通过|成功|如上|实测|结束|完毕)/;
        return !doneWords.test(tail0.slice(-30));
    }
    return false;
}

function resolveSendTarget({ target = '', receiveIdType = '', session = '', topicId = '' } = {}) {
    if (target) {
        return { target, receiveIdType: inferReceiveIdType(target, receiveIdType) };
    }

    const bridgeConfig = loadBridgeConfig();
    const agent = findAgent(bridgeConfig.bindAgent);
    if (!agent) throw new Error(`未找到绑定 Agent: ${bridgeConfig.bindAgent}`);

    const topic = findFeishuTopic(agent.config, topicId || session);
    if (!topic) throw new Error('缺少 target，且无法通过 session/topic_id 找到飞书会话');
    const meta = topic._metadata || {};
    const resolvedTarget = meta.targetId || meta.chatId || meta.userId;
    if (!resolvedTarget) throw new Error(`话题 ${topic.id} 缺少飞书目标元数据`);

    return {
        target: resolvedTarget,
        receiveIdType: inferReceiveIdType(resolvedTarget, receiveIdType || meta.receiveIdType),
        sessionKey: meta.sessionKey || null,
        topicId: topic.id,
    };
}

async function sendMessage(target, content, receiveIdType, options = {}) {
    const resolved = resolveSendTarget({
        target: String(target || '').trim(),
        receiveIdType: String(receiveIdType || '').trim(),
        session: String(options.session || options.sessionKey || '').trim(),
        topicId: String(options.topicId || options.topic_id || '').trim(),
    });
    const result = await sendFeishuText(resolved.target, content, { receiveIdType: resolved.receiveIdType });
    return {
        ...result,
        resolvedTarget: resolved.target,
        receiveIdType: resolved.receiveIdType,
        sessionKey: resolved.sessionKey,
        topicId: resolved.topicId,
    };
}

async function initialize(pluginConfig = {}) {
    configure(pluginConfig);
    stats.connected = false;
    stats.startedAt = new Date().toISOString();
    stats.lastError = null;

    const bridgeConfig = loadBridgeConfig();
    const missing = [];
    if (!bridgeConfig.appId) missing.push('FeishuAppId');
    if (!bridgeConfig.appSecret) missing.push('FeishuAppSecret');
    if (!bridgeConfig.bindAgent) missing.push('FeishuBindAgent');
    if (missing.length > 0) {
        const err = new Error(`配置缺失: ${missing.join(', ')}`);
        setLastError(err);
        throw err;
    }
    if (!findAgent(bridgeConfig.bindAgent)) {
        const err = new Error(`未找到绑定 Agent: ${bridgeConfig.bindAgent}`);
        setLastError(err);
        throw err;
    }

    try {
        lark = require('@larksuiteoapi/node-sdk');
    } catch (err) {
        setLastError(err);
        throw new Error(`加载 @larksuiteoapi/node-sdk 失败: ${err.message}`);
    }

    const eventDispatcher = new lark.EventDispatcher({}).register({
        'im.message.receive_v1': async data => {
            try {
                await handleFeishuEvent(data);
            } catch (err) {
                setLastError(err);
                warn('消息处理异常:', err.message);
            }
        },
        // v34: 审批卡按钮回调——SDK事件键名为card.action.trigger(无ed)
        'card.action.trigger': async data => handleApprovalCardAction(data),
        'card.action.triggered': async data => handleApprovalCardAction(data), // 双注册兜底
    });

    try {
        let readySettled = false;
        let resolveReady;
        let rejectReady;
        const readyPromise = new Promise((resolve, reject) => {
            resolveReady = resolve;
            rejectReady = reject;
        });
        const settleReady = (err) => {
            if (readySettled) return;
            readySettled = true;
            if (err) rejectReady(err);
            else resolveReady();
        };
        const readyTimeout = setTimeout(() => {
            settleReady(new Error(`飞书 WebSocket 连接超时（${FEISHU_WS_READY_TIMEOUT_MS}ms）`));
        }, FEISHU_WS_READY_TIMEOUT_MS);

        wsClient = new lark.WSClient({
            appId: bridgeConfig.appId,
            appSecret: bridgeConfig.appSecret,
            loggerLevel: debugMode ? lark.LoggerLevel.debug : lark.LoggerLevel.warn,
            autoReconnect: true,
            onReady: () => {
                try { startTimedResultsWatcher(); } catch (e) { warn('[TimedRelay] 启动挂载失败:', e.message); }
                try { bindApprovalBridge(); } catch (e) { warn('[审批卡] 绑定失败:', e.message); }
                stats.connected = true;
                settleReady();
            },
            onError: err => {
                stats.connected = false;
                setLastError(err);
                settleReady(err);
                warn('WebSocket 错误:', err.message);
            },
            onReconnecting: () => { stats.connected = false; },
            onReconnected: () => { stats.connected = true; },
            handshakeTimeoutMs: 30000,
            wsConfig: { pingTimeout: 90 },
        });
        await wsClient.start({ eventDispatcher });
        await readyPromise.finally(() => clearTimeout(readyTimeout));
        log(`WebSocket 已连接，绑定 Agent=${bridgeConfig.bindAgent}`);
        reportOrphanedInflight(); // v10.2 重启孤儿检测（重启杀在途回复的显式反馈）
    } catch (err) {
        stats.connected = false;
        setLastError(err);
        shutdown();
        throw err;
    }
}

function shutdown() {
    try {
        if (wsClient && typeof wsClient.close === 'function') wsClient.close();
    } catch (err) {
        warn('关闭 WebSocket 异常:', err.message);
    }
    wsClient = null;
    stats.connected = false;
    tenantTokenCache = { appId: '', appSecret: '', token: '', expiresAt: 0 };
}

function getStatus() {
    const bridgeConfig = loadBridgeConfig();
    const settings = loadSettings();
    return {
        ...stats,
        connected: stats.connected,
        appId: bridgeConfig.appId ? `${bridgeConfig.appId.slice(0, 8)}***` : null,
        bindAgent: bridgeConfig.bindAgent || null,
        systemSettings: {
            vcpServerUrl: settings.vcpServerUrl || null,
            enableVcpToolInjection: settings.enableVcpToolInjection === true,
        },
    };
}

module.exports = {
    configure,
    initialize,
    shutdown,
    sendMessage,
    getStatus,
    listFeishuTopics,
    listFeishuGroups,
    listFeishuTopicsDetailed,
    listFeishuGroupsDetailed,
};
