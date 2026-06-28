# Cangjie MQTT Client

用华为仓颉语言实现的 MQTT 客户端，支持 **MQTT v3.1.1** 和 **MQTT v5.0** 协议，提供完整的发布/订阅功能。

---

## 功能特性

### 已实现

- CONNECT/CONNACK 握手（v3.1.1 + v5.0）
- PUBLISH（QoS 0 / QoS 1 / QoS 2）
- PUBACK / PUBREC / PUBREL / PUBCOMP 确认机制
- SUBSCRIBE / SUBACK（支持多主题）
- UNSUBSCRIBE / UNSUBACK
- PINGREQ / PINGRESP Keep Alive 定时器（含超时检测）
- DISCONNECT（v3.1.1 + v5.0 两种格式）
- 主题通配符匹配（`+` 单级 / `#` 多级 / `$SYS` 系统主题过滤，含空层级拒绝）
- Per-subscription 回调 + 全局 onMessage 兜底
- Inflight 消息存储（QoS 1/2 超时重传，DUP 标志设置，最多 3 次重试）
- TCP 传输层（异步接收线程）
- QoS 2 接收端完整四次握手（PUBREC → PUBREL → PUBCOMP），含去重
- CONNACK 超时等待（10 秒）
- ConnectionError / ProtocolError 异常类型
- **TLS/SSL 加密传输**（`broker.emqx.io:8883`）
- **WebSocket 传输**（`broker.emqx.io:8083`，MQTT over WebSocket）
- **持久会话**（`cleanStart=false`，broker 返回 session present，恢复订阅）
- **自动重连**（Keep Alive 超时自动触发，指数退避，会话感知重连，in-flight 消息重传）
- **v5.0 Properties 完整编解码**（Session Expiry, Receive Maximum, Payload Format, User Property, Reason String, Auth Method/Data, Content Type, Will Delay 等 27 种属性）
- **v5.0 Topic Alias 接收侧解析**
- **协议版本切换**（`ConnectOptions.protocolVersion`）
- **线程安全**（Mutex + synchronized 保护全部共享状态）

---

## 快速开始

### 前置要求

- 华为仓颉 SDK 1.1.3+（从华为开发者联盟获取）
- 无需本地 MQTT Broker（可直接使用公开测试服务）

### 编译与测试

```bash
# 编译
cjpm build

# 运行单元测试（109 项，离线）
cjpm test

# 运行集成测试（连接 broker.emqx.io:1883, 需约 60 秒）
cd integration && cjpm run
```

### 集成测试预期输出

```
=== MQTT 集成测试套件 ===
--- 测试 1: 基础连接与断开 ---  PASS
--- 测试 2: 消息接收（双客户端）---  PASS
--- 测试 3: QoS 2 发布/接收 ---    PASS
--- 测试 4: 取消订阅 ---           PASS
--- 测试 5: Retained 消息 ---      PASS
--- 测试 6: TLS 加密连接 ---       PASS
=== 结果: 6 通过, 0 失败 ===
```

---

## 项目结构

```
src/
├── main.cj                    # 入口 / 集成测试
├── basic_test.cj              # 基础类型单元测试（17 项）
├── client/
│   └── mqtt_client.cj         # MqttClient 主类
├── integration/               # 集成测试项目（独立可执行）
│   ├── cjpm.toml
│   └── src/main.cj
├── protocol/
│   ├── types.cj               # 协议类型（PacketType/QoS/Result/Properties 等）
│   ├── codec.cj               # 编解码基类（Template Method 模式）
│   ├── v311_codec.cj          # MQTT v3.1.1 编解码实现
│   ├── v50_codec.cj           # MQTT v5.0 编解码实现
│   ├── encoder.cj             # ByteArrayOutputStream 辅助
│   ├── decoder.cj             # MqttDecoder + ByteArrayInputStream
│   ├── results.cj             # 统一解析结果类型（ConnAckResult 等）
│   ├── topic_matcher.cj       # 主题通配符匹配器
│   ├── topic_matcher_test.cj  # 通配符测试（12 项）
│   ├── encoding_test.cj       # 协议编解码测试（73 项）
│   └── properties_test.cj     # v5.0 Properties 测试（7 项）
├── transport/
│   ├── tcp_transport.cj       # TCP/TLS 传输层
│   └── ws_transport.cj        # WebSocket 传输层
└── store/
    └── message_store.cj       # Inflight 消息存储（HashMap 实现）
```

---

## API 使用示例

### 连接并订阅

```cangjie
let options = ConnectOptions(
    "my-client",
    "broker.emqx.io",
    port: 1883
)
options.cleanStart = true
options.keepAlive = 30

let client = MqttClient(options)

client.onConnect = { =>
    println("连接成功")
}

client.onMessage = { topic, payload =>
    println("收到 [${topic}]: ${String.fromUtf8(payload)}")
}

client.onError = { error =>
    println("错误: ${error}")
}

client.connect()

// 订阅主题
client.subscribe("test/#", qos: QoS.AT_LEAST_ONCE)

// 发布消息
client.publish("test/hello", "Hello MQTT", qos: QoS.AT_MOST_ONCE)

// 断开连接
client.disconnect()
```

### 带认证的连接

```cangjie
let options = ConnectOptions("auth-client", "broker.emqx.io")
options.username = Some("user")
options.password = Some(stringToUtf8("pass"))
client.connect()
```

### 切换协议版本

```cangjie
let options = ConnectOptions("v5-client", "broker.emqx.io")
options.protocolVersion = ProtocolVersion.V5_0
```

### Will Message（遗嘱消息）

```cangjie
let options = ConnectOptions("will-client", "broker.emqx.io")
options.willTopic = Some("clients/will-client/status")
options.willPayload = Some(stringToUtf8("offline"))
options.willQos = QoS.AT_LEAST_ONCE
options.willRetain = true
```

### 自动重连

```cangjie
let options = ConnectOptions("reconnect-client", "broker.emqx.io")
options.autoReconnect = true
options.maxReconnectAttempts = 5
options.reconnectBaseDelayMs = 1000  // 起始 1 秒，指数退避，最大 30 秒
client.connect()
// 连接意外断开后自动重连，恢复订阅和 in-flight 消息
```

### v5.0 Properties

```cangjie
let options = ConnectOptions("v5-props", "broker.emqx.io")
options.protocolVersion = ProtocolVersion.V5_0
// 设置 CONNECT 属性
options.properties.sessionExpiryInterval = Some(3600)
options.properties.receiveMaximum = Some(100)
// 设置遗嘱属性
options.willTopic = Some("status")
options.willProperties.willDelayInterval = Some(30)  // 30 秒延迟
options.willProperties.contentType = Some("text/plain")
```

---

## 测试覆盖

| 类型 | 命令 | 数量 | 覆盖范围 |
|------|------|------|---------|
| 单元测试 | `cjpm test` | 109 | 枚举映射、编解码 roundtrip、VBI 边界、主题匹配、消息存储、Properties |
| 集成测试 | `cd integration && cjpm run` | 6 | 连接/断开、双客户端消息接收、QoS 2、取消订阅、Retained、TLS |

### 测试文件

- `src/basic_test.cj` — PacketType/QoS/ProtocolVersion 枚举、ConnectOptions 构造、Result 类型、InflightMessage、MessageStore
- `src/protocol/encoding_test.cj` — CONNECT v3.1.1/v5.0、PUBLISH QoS 0/1/2、PUBACK/PUBREC/PUBREL/PUBCOMP roundtrip、CONNACK 解析、DISCONNECT v3.1.1/v5.0、AUTH roundtrip、畸形包拒绝
- `src/protocol/properties_test.cj` — v5.0 Properties 编解码（sessionExpiry、receiveMax、payloadFormat、userProperty 等）
- `src/protocol/topic_matcher_test.cj` — 精确匹配、`+`/`#` 通配符、空层级拒绝、`$SYS` 过滤、合法性校验

---

## 协议版本对比

| 特性 | v3.1.1 | v5.0 |
|------|---------|-------|
| Properties | 无 | 27 种属性编解码 |
| Packet Format | 无 Properties Length | 含 Properties Length |
| DISCONNECT | `0xE0 0x00` | 含 Reason Code + Properties |
| CONNECT Protocol Level | 4 | 5 |
| Will Properties | 无 | Will Delay / Content Type |
| 认证 | 基础 | 增强认证 (AUTH) |
| Topic Alias | 无 | 接收侧解析 |
| Reason Code | 6 个 | 43 个 |
| SUBSCRIBE Options | QoS | QoS + No Local / Retain As Published / Retain Handling |

---

## 参考资源

- [MQTT v3.1.1 Specification](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)
- [MQTT v5.0 Specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)
- 本项目协议摘要: `docs/MQTT-v3.1.1-summary.md` / `docs/MQTT-v5.0-summary.md`

---

**注意**: 这是一个学习项目。集成测试依赖公开测试服务 `broker.emqx.io:1883`，可能受网络条件和该服务可用性影响。
