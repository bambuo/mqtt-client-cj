# MQTT v5.0 协议规范摘要

本文档摘录 MQTT v5.0 协议规范中的核心内容，便于快速参考。

**完整规范**: [MQTT v5.0 OASIS Standard](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)

---

## 目录

1. [协议概述](#协议概述)
2. [包格式](#包格式)
3. [控制包类型](#控制包类型)
4. [QoS 等级](#qos-等级)
5. [主题和通配符](#主题和通配符)
6. [连接流程](#连接流程)
7. [发布流程](#发布流程)
8. [订阅流程](#订阅流程)
9. [Keep Alive](#keep-alive)
10. [会话状态](#会话状态)

---

## 协议概述

**MQTT (Message Queuing Telemetry Transport)** 是一种轻量级的发布/订阅消息传输协议，设计用于资源受限的设备和低带宽、高延迟或不稳定的网络环境。

### 核心特性

- **发布/订阅模式** - 解耦生产者与消费者
- **QoS 等级** - 保证消息交付质量
- **小巧传输** - 最小化协议头开销
- **Last Will** - 遗嘱消息机制
- **Retained** - 保留消息供新订阅者接收

---

## 包格式

每个 MQTT 控制包由三部分组成：

```
+-------------------+
| Fixed Header      |  2 bytes (所有包必须有)
+-------------------+
| Variable Header   |  Optional (包类型相关)
+-------------------+
| Payload           |  Optional (包类型相关)
+-------------------+
```

### Fixed Header 格式

```
Bit:  7  6  5  4  |  3  2  1  0
     +---------------+---------------+
     | Packet Type   |    Flags      |
     +---------------+---------------+
     | Remaining Length (Variable Byte Integer)
     +-------------------------------+
```

**Packet Type (4 bits)**:

| 值 | 包类型 | 方向 |
|----|--------|------|
| 1 | CONNECT | C→S |
| 2 | CONNACK | S→C |
| 3 | PUBLISH | C↔S |
| 4 | PUBACK | C↔S |
| 5 | PUBREC | C↔S |
| 6 | PUBREL | C↔S |
| 7 | PUBCOMP | C↔S |
| 8 | SUBSCRIBE | C→S |
| 9 | SUBACK | S→C |
| 10 | UNSUBSCRIBE | C→S |
| 11 | UNSUBACK | S→C |
| 12 | PINGREQ | C→S |
| 13 | PINGRESP | S→C |
| 14 | DISCONNECT | C→S |
| 15 | AUTH | C↔S |

**Remaining Length (可变字节整数)**:
- 1-4 字节编码
- 每个字节的 bit 7 是延续位
- 最大值: 268,435,455 (256MB)

---

## 控制包详解

### 1. CONNECT (客户端 → Broker)

**Fixed Header**: `0x10` + Remaining Length

**Variable Header**:

```
+-------------------+
| Protocol Name     |  "MQTT" (v5)
+-------------------+
| Protocol Level    |  5 (v5.0)
+-------------------+
| Connect Flags     |  1 byte
+-------------------+
| Keep Alive        |  2 bytes
+-------------------+
| Properties        |  Variable Byte Integer + Properties
+-------------------+
```

**Connect Flags**:

```
Bit:  7      6      5      4      3   2   1      0
     +------+------+------+------+---+---+------+
     | User | Pass | Will | Will | Will | Clean |
     | name | word | Retain| QoS  | QoS| Start |
     +------+------+------+------+---+---+------+
```

**Payload** (按顺序):
1. Client ID (必须)
2. Will Properties (可选)
3. Will Topic (如果 Will Flag = 1)
4. Will Message (如果 Will Flag = 1)
5. Username (如果 Username Flag = 1)
6. Password (如果 Password Flag = 1)

### 2. CONNACK (Broker → 客户端)

**Fixed Header**: `0x20` + Remaining Length

**Variable Header**:

```
+-------------------+
| Session Present   |  1 byte (bit 0)
+-------------------+
| Reason Code       |  1 byte
+-------------------+
| Properties        |  Variable Byte Integer + Properties
+-------------------+
```

**Reason Code**:

| 值 | 含义 |
|----|------|
| 0 | 成功接受 |
| 1 | 协议版本错误 |
| 2 | 客户端 ID 无效 |
| 3 | 服务不可用 |
| 4 | 用户名或密码错误 |
| 5 | 未授权 |

### 3. PUBLISH (双向)

**Fixed Header**: `0x30` + Flags + Remaining Length

**Flags**:
- Bit 3: DUP (重传标志)
- Bit 2,1: QoS Level
- Bit 0: Retain

**Variable Header**:
1. Topic Name (长度 + 内容)
2. Packet ID (QoS > 0 时需要)
3. Properties

**Payload**: 应用消息内容 (可选)

### 4. PUBACK (双向)

**Variable Header**:
1. Packet ID
2. Reason Code
3. Properties

### 5. SUBSCRIBE (客户端 → Broker)

**Variable Header**:
1. Packet ID
2. Properties

**Payload** (可多个订阅):
```
+-------------------+
| Topic Filter      |  (长度 + 内容)
+-------------------+
| Subscription Options |  1 byte
+-------------------+
```

**Subscription Options**:
- Bit 2,1,0: QoS
- Bit 3: No Local
- Bit 4: Retain As Published
- Bit 5,6: Retain Handling

---

## QoS 等级

### QoS 0 - At most once (最多一次)

```
发布者 → PUBLISH (QoS 0) → Broker
```

- 不保证交付
- 无确认机制
- 适用于实时数据、传感器数据

### QoS 1 - At least once (至少一次)

```
发布者 → PUBLISH (QoS 1) → Broker
       ← PUBACK ← Broker
```

- 保证消息到达，但可能重复
- 需要存储消息直到收到 PUBACK
- 超时未收到 PUBACK 需重传

### QoS 2 - Exactly once (恰好一次)

```
发布者 → PUBLISH → Broker
       ← PUBREC ← Broker
       
发布者 → PUBREL → Broker
       ← PUBCOMP ← Broker
```

- 保证消息仅到达一次
- 最严格，开销最大
- 需要持久化中间状态

---

## 主题和通配符

### 主题格式

- **层级分隔符**: `/` (如 `home/bedroom/temperature`)
- **区分大小写**: `Home` ≠ `home`
- **长度限制**: 最大 65535 字节 (UTF-8)

### 通配符

#### `+` (单级通配符)

匹配一个层级：

```
订阅: home/+/temperature
匹配: home/bedroom/temperature    ✅
匹配: home/livingroom/temperature  ✅
匹配: home/bedroom/1/temperature  ❌ (多级)
```

#### `#` (多级通配符)

匹配零个或多个层级 (必须在末尾)：

```
订阅: home/#
匹配: home                        ✅
匹配: home/bedroom                ✅
匹配: home/bedroom/temperature    ✅

订阅: home/bedroom/#
匹配: home/bedroom                ✅
匹配: home/bedroom/temperature    ✅
匹配: home/livingroom             ❌ (不匹配)
```

### 系统主题 (Broker 特定)

以 `$` 开头，通配符不匹配：

```
$SYS/broker/clients/connected
```

---

## 连接流程

### 典型连接流程

```
客户端                           Broker
  │                               │
  │──── CONNECT ────────────────>│
  │                               │
  │<──── CONNACK ───────────────│
  │                               │
  │──── SUBSCRIBE ─────────────>│
  │<──── SUBACK ────────────────│
  │                               │
  │──── PUBLISH (QoS 0) ──────>│
  │                               │
  │<──── PUBLISH ───────────────│
  │                               │
  │──── DISCONNECT ────────────>│
  │                               │
```

### Clean Start (v5) / Clean Session (v3.1.1)

| Clean Start | Session Present | 行为 |
|-------------|-----------------|------|
| 0 | 0 | 创建新会话 |
| 0 | 1 | 恢复旧会话 |
| 1 | 0 | 创建新会话 |
| 1 | 1 | 删除旧会话，创建新会话 |

---

## Keep Alive

### 机制

1. 客户端在 CONNECT 中指定 `Keep Alive` (秒)
2. 如果 `Keep Alive > 0`:
   - 客户端在 `Keep Alive * 0.5` 秒内未发送任何包，必须发送 PINGREQ
   - Broker 在 `Keep Alive * 1.5` 秒内未收到任何包，应断开连接
3. 如果 `Keep Alive = 0`，禁用 Keep Alive

### PINGREQ/PINGRESP

```
客户端 ──── PINGREQ ────> Broker
客户端 <─── PINGRESP ──── Broker
```

- PINGREQ 和 PINGRESP 没有 Variable Header 或 Payload
- Remaining Length = 0

---

## 会话状态

### 客户端会话状态

- 所有已订阅的主题过滤器
- QoS 1/2 的 inflight 消息
- 等待确认的 QoS 1/2 消息
- 待重传的消息

### Broker 会话状态

- 客户端的订阅
- QoS 1/2 的 inflight 消息 (未确认)
- 尚未分发给客户端的质量 > 0 的消息
- (可选) 尚未分发的 QoS 0 消息

---

##  Properties (v5 新增)

MQTT v5.0 引入了 Properties，每个控制包都可以携带属性。

### 常见属性

| 属性 ID | 属性名 | 适用包 |
|---------|--------|--------|
| 0x01 | Payload Format Indicator | PUBLISH |
| 0x02 | Message Expiry Interval | PUBLISH |
| 0x08 | Response Topic | PUBLISH |
| 0x09 | Correlation Data | PUBLISH |
| 0x0B | Subscription Identifier | SUBSCRIBE |
| 0x11 | Session Expiry Interval | CONNECT, CONNACK |
| 0x13 | Receive Maximum | CONNECT, CONNACK |
| 0x1F | Request Problem Information | CONNECT |
| 0x21 | Will Delay Interval | CONNECT |

### Properties 编码格式

```
+-----------------------+
| Properties Length     |  Variable Byte Integer
+-----------------------+
| Property ID (1 byte)  |
+-----------------------+
| Property Value        |  (长度取决于属性类型)
+-----------------------+
| ... (多个属性)        |
+-----------------------+
```

---

## 错误码 (Reason Code)

MQTT v5.0 为大多数包引入了 Reason Code。

### 常见 Reason Code

| 值 | 名称 | 含义 |
|----|------|------|
| 0x00 | Success | 成功 |
| 0x04 | Disconnect with Will Message | 发送遗嘱消息后断开 |
| 0x10 | No matching subscribers | 无匹配的订阅者 |
| 0x11 | No subscription existed | 订阅不存在 |
| 0x80 | Unspecified error | 未指定错误 |
| 0x81 | Malformed Packet | 包格式错误 |
| 0x82 | Protocol Error | 协议错误 |
| 0x8D | Keep Alive timeout | Keep Alive 超时 |
| 0x95 | Packet too large | 包过大 |

---

## 实现检查清单

### 全部已实现

- [x] CONNECT/CONNACK 编码/解码（v3.1.1 + v5.0）
- [x] PUBLISH (QoS 0/1/2) 编码/解码，含 DUP/Retain 标志位
- [x] SUBSCRIBE/SUBACK 编码/解码（v5.0 订阅选项字节 QoS<<1）
- [x] UNSUBSCRIBE/UNSUBACK 编码/解码
- [x] DISCONNECT 编码/解码（v3.1.1 + v5.0 ReasonCode/Properties）
- [x] PUBACK/PUBREC/PUBREL/PUBCOMP 编解码（QoS 1/2 四次握手）
- [x] PINGREQ/PINGRESP + Keep Alive 超时检测
- [x] AUTH 编码/解码（v5.0 only）
- [x] TCP + TLS + WebSocket 传输层
- [x] 主题通配符匹配（+ / # / $SYS / 空层级拒绝）
- [x] Will Message（遗嘱消息，含 v5.0 Will Properties）
- [x] Retained 消息
- [x] 持久会话（cleanStart=false, session present 恢复）
- [x] v5.0 Properties 编解码（27 种属性）
- [x] v5.0 Topic Alias 接收侧解析
- [x] 自动重连（指数退避，in-flight 重传）
- [x] 线程安全（Mutex + synchronized）

---

**参考**: MQTT v5.0 OASIS Standard  
**整理日期**: 2026-06-27  
**维护者**: mqtt-client-cj 项目
