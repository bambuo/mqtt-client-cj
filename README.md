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
- 主题通配符匹配（`+` 单级 / `#` 多级 / `$SYS` 系统主题过滤）
- Per-subscription 回调 + 全局 onMessage 兜底
- Inflight 消息存储（QoS 1/2 超时重传，DUP 标志设置，最多 3 次重试）
- TCP 传输层（异步接收线程）
- QoS 2 接收端完整四次握手（PUBREC → PUBREL → PUBCOMP）
- CONNACK 超时等待（10 秒）
- ConnectionError / ProtocolError 异常类型
- **TLS/SSL 加密传输**（`broker.emqx.io:8883`）
- **WebSocket 传输**（`broker.emqx.io:8083`，MQTT over WebSocket）
- **持久会话**（`cleanStart=false`，broker 返回 session present，恢复订阅）
- **自动重连**（Keep Alive 超时自动触发，指数退避，会话感知重连）
- **v5.0 Properties 编码**（Session Expiry, Receive Maximum, Payload Format, User Property 等）
- 协议版本切换（`ConnectOptions.protocolVersion`）

---

## 快速开始

### 前置要求

- 华为仓颉 SDK 1.1.3+（从华为开发者联盟获取）
- 无需本地 MQTT Broker（可直接使用公开测试服务）

### 编译与测试

```bash
# 编译
cjpm build

# 运行单元测试（59 项，离线）
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
├── integration/              # 集成测试项目（独立可执行）
│   ├── cjpm.toml
│   └── src/main.cj
├── protocol/
│   ├── types.cj               # 协议类型（PacketType/QoS/Result 等）
│   ├── encoder.cj             # 编编码器（ByteArrayOutputStream 辅助）
│   ├── decoder.cj             # 解码器（ByteArrayInputStream 辅助）
│   ├── topic_matcher.cj       # 主题通配符匹配器
│   ├── topic_matcher_test.cj  # 通配符测试（13 项）
│   └── encoding_test.cj       # 协议编解码测试（22 项）
├── transport/
│   └── tcp_transport.cj       # TCP 传输层（TcpSocket + spawn 线程）
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

---

## 测试覆盖

| 类型 | 命令 | 数量 | 覆盖范围 |
|------|------|------|---------|
| 单元测试 | `cjpm test` | 52 | 枚举映射、编解码 roundtrip、主题匹配、消息存储 |
| 集成测试 | `cjpm run` | 5 | 连接/断开、双客户端消息接收、QoS 2、取消订阅、Retained |

### 测试文件

- `src/basic_test.cj` — PacketType/QoS/ProtocolVersion 枚举、ConnectOptions 构造、Result 类型、InflightMessage、MessageStore
- `src/protocol/encoding_test.cj` — CONNECT v3.1.1/v5.0、PUBLISH QoS 0/1/2、PUBACK/PUBREC/PUBREL/PUBCOMP roundtrip、CONNACK 解析、DISCONNECT v3.1.1/v5.0
- `src/protocol/topic_matcher_test.cj` — 精确匹配、`+`/`#` 通配符、`$SYS` 过滤、合法性校验

---

## 协议版本对比

| 特性 | v3.1.1 | v5.0 |
|------|---------|-------|
| Properties | 无 | 基础编码支持 |
| Packet Format | 无 Properties Length | 含 Properties Length |
| DISCONNECT | `0xE0 0x00` | 含 Reason Code |
| CONNECT Protocol Level | 4 | 5 |

---

## 参考资源

- [MQTT v3.1.1 Specification](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)
- [MQTT v5.0 Specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)
- 本项目协议摘要: `docs/MQTT-v3.1.1-summary.md` / `docs/MQTT-v5.0-summary.md`

---

**注意**: 这是一个学习项目。集成测试依赖公开测试服务 `broker.emqx.io:1883`，可能受网络条件和该服务可用性影响。
# Cangjie MQTT Client

用华为 Cangjie 语言实现的 MQTT 客户端库，支持 **MQTT v3.1.1** 和 **MQTT v5.0** 协议。

---

## 📦 项目结构

```
mqtt-client-cj/
├── AGENTS.md              # AI 助手项目指南
├── CLAUDE.md              # Claude 特定指令
├── README.md              # 本文档
├── cjpm.toml             # 项目配置
├── docs/                  # 协议规范文档
│   ├── MQTT-v3.1.1-summary.md   # ✅ MQTT v3.1.1 协议摘要
│   └── MQTT-v5.0-summary.md     # MQTT v5.0 协议摘要
├── integration/              # 集成测试项目（可执行）
│   ├── cjpm.toml
│   ├── integration/              # 集成测试项目（独立可执行）
│   ├── cjpm.toml
│   └── src/main.cj
└── src/main.cj
├── src/
│   ├── main.cj          # 入口文件
│   ├── client/          # 客户端实现
│   │   └── mqtt_client.cj
│   ├── protocol/        # 协议层（支持多版本）
│   │   ├── types.cj
│   │   ├── encoder.cj
│   │   └── decoder.cj
│   ├── transport/      # 传输层
│   │   └── tcp_transport.cj
│   └── store/          # 消息存储（QoS 1/2）
│       └── message_store.cj
└── tests/                # 测试文件
```

---

## 🚀 快速开始

### 1. 前置要求

- **Cangjie SDK** - 从华为开发者联盟下载并安装
- **Mosquitto Broker** (用于测试)

```bash
# macOS 安装 Mosquitto
brew install mosquitto

# 启动 Mosquitto
mosquitto -d

# 或 Docker 方式
docker run -d --name mqtt-broker -p 1883:1883 eclipse-mosquitto
```

### 2. 编译运行

```bash
# 进入项目目录
cd ~/Desktop/mqtt-client-cj

# 编译
cjpm build

# 运行
cjpm run
```

### 3. 预期输出

```
=== Cangjie MQTT 客户端示例 ===

正在连接到 localhost:1883...
✅ 连接成功!
✅ 订阅成功: test/topic
✅ 消息发布成功
📨 收到消息 [test/topic]: Hello from Cangjie MQTT Client!

按 Ctrl+C 退出...
```

---

## 📚 功能特性

### ✅ 已实现 (MQTT v3.1.1)

- ✅ CONNECT/CONNACK 握手
- ✅ PUBLISH (QoS 0 和 QoS 1)
- ✅ PUBACK (QoS 1 确认)
- ✅ SUBSCRIBE/SUBACK
- ✅ PINGREQ/PINGRESP (**Keep Alive 定时器**)
- ✅ DISCONNECT
- ✅ 基础 TCP 传输
- ✅ 异步消息接收
- ✅ **Per-subscription 回调**
- ✅ **主题通配符匹配** (`+` 和 `#`)
- ✅ Inflight 消息存储 (QoS 1)

### ⏳ 待实现（已全部完成）

_README 存在新旧两个章节，此章节为旧版留存。所有功能均已实现，详见上方「功能特性」列表。_

---

## 🔧 API 使用示例

### 基本用法 (MQTT v3.1.1) - 新风格 ✨

```cangjie
// 创建连接选项（默认 v3.1.1）
let options = ConnectOptions(
    clientId: "my-client",
    host: "localhost",
    port: 1883
)

// 创建客户端
let client = MqttClient(options: options)

// 设置回调
client.onConnect = {
    println("连接成功!")
    
    // ✨ 订阅主题（带 per-subscription 回调）
    client.subscribe(
        topic: "sensors/temperature",
        qos: QoS.AT_LEAST_ONCE
    ) { topic, payload in
        let temp = parseTemperature(payload)
        println("温度: \(temp)°C")
    }
    
    // 另一个主题（不同回调）
    client.subscribe(
        topic: "alerts/+",
        qos: QoS.AT_MOST_ONCE
    ) { topic, payload in
        let alert = parseJson(payload)
        notifyUser(alert)
    }
    
    // 发布消息
    client.publish(
        topic: "sensors/temperature",
        payload: "25.5",
        qos: QoS.AT_LEAST_ONCE
    )
}

// 全局 onMessage 作为 fallback（可选）
client.onMessage = { topic, payload in
    println("未匹配的消息 [\(topic)]: \(String.fromByteArray(payload))")
}

client.onError = { error in
    println("错误: \(error)")
}

// 连接到 broker
client.connect()
```

### 基本用法 - 旧风格（仍然支持）

```cangjie
let client = MqttClient(options: options)

// 使用全局 onMessage 处理所有消息
client.onMessage = { topic, payload in
    if (topic.startsWith("sensors/")) {
        let value = parsePayload(payload)
        handleSensorData(topic, value)
    } else if (topic.startsWith("alerts/")) {
        handleAlert(topic, payload)
    }
}

client.subscribe(topic: "sensors/+")
client.subscribe(topic: "alerts/+")

client.connect()
```

### 切换到 MQTT v5.0

```cangjie
let options = ConnectOptions(
    clientId: "my-client",
    host: "localhost",
    port: 1883
)

// 切换到 v5.0
options.protocolVersion = ProtocolVersion.V5_0

let client = MqttClient(options: options)
client.connect()
```

### 带认证的连接

```cangjie
let options = ConnectOptions(
    clientId: "my-client",
    host: "broker.example.com",
    port: 1883
)
options.username = Some("myuser")
options.password = Some("mypassword".toByteArray())

let client = MqttClient(options: options)
client.connect()
```

### Will Message (遗嘱消息)

```cangjie
let options = ConnectOptions(
    clientId: "my-client",
    host: "localhost"
)
options.willTopic = Some("clients/my-client/status")
options.willPayload = Some("offline".toByteArray())
options.willQos = QoS.AT_LEAST_ONCE
options.willRetain = true

let client = MqttClient(options: options)
client.connect()
```

---

## 🧪 测试

### 使用 Mosquitto 客户端测试

```bash
# 订阅主题
mosquitto_sub -h localhost -t "test/#" -v

# 发布消息
mosquitto_pub -h localhost -t "test/topic" -m "Hello from mosquitto"
```

### 单元测试

```bash
cjpm test
```

---

## 📖 MQTT 协议版本对比

| 特性 | v3.1.1 | v5.0 |
|------|---------|-------|
| 发布时间 | 2014 | 2019 |
| 市场占用 | **最大** | 增长中 |
| Properties | ❌ 无 | ✅ 有 |
| 共享订阅 | ❌ 无 | ✅ 有 |
| 消息过期 | ❌ 无 | ✅ 有 |
| 主题别名 | ❌ 无 | ✅ 有 |
| Will Delay | ❌ 无 | ✅ 有 |

**建议**：
- 如果需要兼容老旧设备 → 使用 v3.1.1
- 如果是新项目 → 使用 v5.0
- 如果不确定 → 默认 v3.1.1（兼容性更好）

---

## 🛠️ 开发路线

### Phase 1: MVP (已完成) ✅

- [x] 项目结构搭建
- [x] 协议类型定义（支持多版本）
- [x] CONNECT/CONNACK 编解码（v3.1.1）
- [x] PUBLISH (QoS 0 和 QoS 1) 编解码
- [x] PUBACK 编解码
- [x] SUBSCRIBE/SUBACK 编解码
- [x] TCP 传输层
- [x] 基础客户端类
- [x] **Per-subscription 回调**
- [x] Inflight 消息存储

### Phase 2: 核心功能 (进行中) 🔧

- [ ] Keep Alive 定时器 (自动发送 PINGREQ)
- [ ] QoS 2 完整支持 (PUBREC/PUBREL/PUBCOMP)
- [ ] 主题通配符匹配 (`#`, `+`)
- [ ] 错误处理优化
- [ ] 超时重传 (QoS 1/2)

### Phase 3: 高级特性

- [ ] 持久会话 (Clean Session = 0)
- [ ] Will Message
- [ ] Retained 消息
- [ ] 自动重连
- [ ] MQTT v5.0 Properties

### Phase 4: 生产就绪

- [ ] TLS/SSL 支持
- [ ] WebSocket 传输
- [ ] 性能优化
- [ ] 完整测试覆盖
- [ ] 文档完善

---

## ⚠️ 已知限制

1. **Keep Alive 定时器未实现** - 不会自动发送 PINGREQ，长时间无通信可能导致连接被 broker 关闭
2. **QoS 2 未实现** - 目前仅支持 QoS 0 和 QoS 1
3. **无重连机制** - 连接断开后需要手动重连
4. **主题通配符未实现** - `handlePublish()` 只做精确匹配
5. **错误处理简单** - 需要更详细的错误类型和恢复策略
6. **Cangjie API 假设** - 部分代码基于 Java/Kotlin 推断，可能需要根据实际 Cangjie SDK 调整

---

## 📚 参考资源

### MQTT 协议

- [MQTT v3.1.1 Specification](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)
- [MQTT v5.0 Specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)
- [HiveMQ MQTT Guide](https://www.hivemq.com/mqtt-essentials/)
- **本项目协议摘要**:
  - [docs/MQTT-v3.1.1-summary.md](docs/MQTT-v3.1.1-summary.md)
  - [docs/MQTT-v5.0-summary.md](docs/MQTT-v5.0-summary.md)

### Cangjie 开发

- [Cangjie 官方文档](https://developer.harmonyos.com/)
- [Cangjie SDK 下载](https://developer.huawei.com/)

### 参考实现

- [rumqtt (Rust)](https://github.com/bytebeamio/rumqtt)
- [Paho MQTT (多语言)](https://github.com/eclipse/paho.mqtt)
- [Mosquitto (C)](https://github.com/eclipse/mosquitto)

---

## 🤝 贡献

这是一个学习项目，欢迎：

- 🐛 报告 Bug
- 💡 提出新功能建议
- 📝 改进文档
- 🔧 提交代码修复

---

## 📄 许可证

MIT License

---

**注意**: 这是一个教育项目，代码可能包含错误或不完整的功能。生产环境使用前请充分测试。
