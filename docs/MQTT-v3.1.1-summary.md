# MQTT v3.1.1 协议规范摘要

> 基于 OASIS Standard (2014) 编写  
> 官方规范: https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html

---

## 目录

1. [协议概述](#协议概述)
2. [包格式](#包格式)
3. [控制包类型](#控制包类型)
4. [CONNECT / CONNACK](#connect--connack)
5. [PUBLISH / PUBACK](#publish--puback)
6. [SUBSCRIBE / SUBACK](#subscribe--suback)
7. [PINGREQ / PINGRESP](#pingreq--pingresp)
8. [DISCONNECT](#disconnect)
9. [QoS 等级](#qos-等级)
10. [主题通配符](#主题通配符)
11. [与 v5.0 的主要差异](#与-v50-的主要差异)

---

## 协议概述

**MQTT (Message Queuing Telemetry Transport)** 是一种轻量级的发布/订阅消息传输协议，专为受限设备和低带宽、高延迟或不可靠的网络设计。

### 核心特性

- **发布/订阅模式** - 解耦生产者和消费者
- **小型传输开销** - 最小包头部仅 2 字节
- **QoS 等级** - 提供 3 种消息传递保证
- **Last Will and Testament (LWT)** - 遗嘱消息
- **Retained Messages** - 保留消息
- **Keep Alive** - 客户端-服务端心跳

### 协议版本

| 版本 | 发布时间 | 说明 |
|------|----------|------|
| v3.1 | 2010 | IBM 原始版本 |
| **v3.1.1** | **2014** | **OASIS 标准化，最广泛使用的版本** |
| v5.0 | 2019 | 新增 Properties、共享订阅等特性 |

---

## 包格式

每个 MQTT 控制包都遵循以下格式：

```
+-------------------+
| Fixed Header      |  1 byte (必需)
+-------------------+
| Remaining Length  |  1-4 bytes (可变字节整数)
+-------------------+
| Variable Header   |  Optional (包类型相关)
+-------------------+
| Payload           |  Optional (包类型相关)
+-------------------+
```

### Fixed Header (固定头部)

```
Bit:  7  6  5  4  |  3  2  1  0
     --------------|----------------
     Packet Type   |  Flags
```

**Packet Type (4 bits)**:
```
1 = CONNECT
2 = CONNACK
3 = PUBLISH
4 = PUBACK
5 = PUBREC
6 = PUBREL
7 = PUBCOMP
8 = SUBSCRIBE
9 = SUBACK
10 = UNSUBSCRIBE
11 = UNSUBACK
12 = PINGREQ
13 = PINGRESP
14 = DISCONNECT
```

**Flags (4 bits)**:
- 对于 PUBLISH 包：
  - Bit 3: DUP (重传标志)
  - Bit 2-1: QoS 等级
  - Bit 0: Retain (保留标志)

### Remaining Length (剩余长度)

使用 **可变字节整数 (Variable Byte Integer)** 编码：
- 1-4 字节
- 每个字节的低 7 位存储数据
- 最高位 (MSB) 是 "继续位" (1 = 还有更多字节)
- 最大值: 268,435,455 (256 MB)

**编码示例**:
```
Remaining Length = 321 (0x141)
字节 1: 0xC1  (321 % 128 = 65, 继续位 = 1)
字节 2: 0x02  (321 / 128 = 2, 继续位 = 0)
```

---

## 控制包类型

### CONNECT (客户端 → 服务端)

**用途**: 客户端请求连接到 MQTT 服务端

**Fixed Header**:
```
Byte 0: 0x10  (Packet Type = 1, Flags = 0x00)
```

**Variable Header**:
```
+-------------------+
| Protocol Name     |  6 bytes ("MQTT")
+-------------------+
| Protocol Level    |  1 byte (4 = v3.1.1)
+-------------------+
| Connect Flags     |  1 byte
+-------------------+
| Keep Alive       |  2 bytes
+-------------------+
```

**Connect Flags**:
```
Bit:  7      6      5      4      3      2      1      0
     User  Pass  Will  Will  Will  Will  Clean  (Reserved)
     Name  Word  Retain QoS2  QoS1  Flag  Start
```

- **Clean Start** (Bit 1): 
  - 1 = 丢弃旧会话，创建新会话
  - 0 = 恢复旧会话（如果存在）
- **Will Flag** (Bit 2): 是否设置遗嘱消息
- **Will QoS** (Bit 3-4): 遗嘱消息的 QoS 等级
- **Will Retain** (Bit 5): 遗嘱消息是否保留
- **Password Flag** (Bit 6): 是否包含密码
- **Username Flag** (Bit 7): 是否包含用户名

**Payload**:
```
+-------------------+
| Client ID         |  必需
+-------------------+
| Will Topic        |  可选 (Will Flag = 1)
+-------------------+
| Will Message      |  可选 (Will Flag = 1)
+-------------------+
| Username          |  可选 (Username Flag = 1)
+-------------------+
| Password          |  可选 (Password Flag = 1)
+-------------------+
```

**示例 (十六进制)**:
```
10 1C 00 06 4D 51 54 54 04 02 00 3C 00 05 74 65
73 74 31 00 05 68 65 6C 6C 6F
```

解码:
```
10        → CONNECT (0x10)
1C        → Remaining Length = 28 bytes
00 06     → Protocol Name Length = 6
4D 51 54 54 → "MQTT"
04        → Protocol Level = 4 (v3.1.1)
02        → Connect Flags (Clean Start = 1)
00 3C     → Keep Alive = 60 seconds
00 05     → Client ID Length = 5
74 65 73 74 31 → "test1"
00 05     → Payload Length = 5
68 65 6C 6C 6F → "hello"
```

---

### CONNACK (服务端 → 客户端)

**用途**: 确认 CONNECT 请求，返回连接结果

**Fixed Header**:
```
Byte 0: 0x20  (Packet Type = 2, Flags = 0x00)
```

**Variable Header**:
```
+-------------------+
| Acknowledge Flags |  1 byte
+-------------------+
| Return Code       |  1 byte
+-------------------+
```

**Acknowledge Flags**:
```
Bit 0: Session Present
  - 0 = 服务端没有存储的会话状态
  - 1 = 服务端有存储的会话状态
```

**Return Code**:
```
0 = 连接已接受
1 = 不支持的协议版本
2 = 拒绝的客户端标识符
3 = 服务端不可用
4 = 无效的用户名或密码
5 = 未授权
```

**示例**:
```
20 02 00 00
```

解码:
```
20        → CONNACK (0x20)
02        → Remaining Length = 2 bytes
00        → Session Present = 0
00        → Return Code = 0 (连接已接受)
```

---

## PUBLISH / PUBACK

### PUBLISH (客户端 ↔ 服务端)

**用途**: 发布消息到主题

**Fixed Header**:
```
Byte 0: 0x30 (QoS 0) / 0x32 (QoS 1) / 0x34 (QoS 2)
```

**Flags**:
- **DUP (Bit 3)**: 重传标志
- **QoS (Bit 2-1)**: QoS 等级 (0/1/2)
- **Retain (Bit 0)**: 是否保留消息

**Variable Header**:
```
+-------------------+
| Topic Name        |  2 bytes (长度) + UTF-8 字符串
+-------------------+
| Packet ID         |  2 bytes (仅 QoS 1/2)
+-------------------+
```

**Payload**:
- 任意二进制数据（由应用层定义格式）

**示例 (QoS 0)**:
```
30 0D 00 05 74 6F 70 69 63 31 48 65 6C 6C 6F
```

解码:
```
30        → PUBLISH (QoS 0, Retain = 0)
0D        → Remaining Length = 13 bytes
00 05     → Topic Length = 5
74 6F 70 69 63 31 → "topic1"
48 65 6C 6C 6F → "Hello"
```

---

### PUBACK (客户端 ↔ 服务端)

**用途**: 确认 QoS 1 消息已收到

**Fixed Header**:
```
Byte 0: 0x40  (Packet Type = 4, Flags = 0x00)
```

**Variable Header**:
```
+-------------------+
| Packet ID         |  2 bytes
+-------------------+
```

**示例**:
```
40 02 00 01
```

解码:
```
40        → PUBACK (0x40)
02        → Remaining Length = 2 bytes
00 01     → Packet ID = 1
```

---

## SUBSCRIBE / SUBACK

### SUBSCRIBE (客户端 → 服务端)

**用途**: 订阅一个或多个主题

**Fixed Header**:
```
Byte 0: 0x82  (Packet Type = 8, Flags = 0x02)
```

**Variable Header**:
```
+-------------------+
| Packet ID         |  2 bytes
+-------------------+
```

**Payload**:
```
+-------------------+
| Topic Filter 1    |  2 bytes (长度) + UTF-8 字符串
+-------------------+
| Requested QoS 1   |  1 byte (0/1/2)
+-------------------+
| Topic Filter 2    |  ...
+-------------------+
| Requested QoS 2   |  ...
+-------------------+
```

**示例**:
```
82 0A 00 01 00 05 74 6F 70 69 63 31 00
```

解码:
```
82        → SUBSCRIBE (QoS 1)
0A        → Remaining Length = 10 bytes
00 01     → Packet ID = 1
00 05     → Topic Filter Length = 5
74 6F 70 69 63 31 → "topic1"
00        → Requested QoS = 0
```

---

### SUBACK (服务端 → 客户端)

**用途**: 确认 SUBSCRIBE 请求，返回每个主题的 QoS 等级

**Fixed Header**:
```
Byte 0: 0x90  (Packet Type = 9, Flags = 0x00)
```

**Variable Header**:
```
+-------------------+
| Packet ID         |  2 bytes
+-------------------+
```

**Payload**:
```
+-------------------+
| Return Code 1     |  1 byte (QoS 0/1/2 或 0x80 = 失败)
+-------------------+
| Return Code 2     |  ...
+-------------------+
```

**Return Code**:
```
0 = QoS 0
1 = QoS 1
2 = QoS 2
0x80 = 订阅失败
```

**示例**:
```
90 03 00 01 00
```

解码:
```
90        → SUBACK (0x90)
03        → Remaining Length = 3 bytes
00 01     → Packet ID = 1
00        → Topic 1 Return Code = QoS 0
```

---

## PINGREQ / PINGRESP

### PINGREQ (客户端 → 服务端)

**用途**: 心跳请求（保持连接）

**Fixed Header**:
```
Byte 0: 0xC0  (Packet Type = 12, Flags = 0x00)
```

**Remaining Length**: 0

**示例**:
```
C0 00
```

---

### PINGRESP (服务端 → 客户端)

**用途**: 心跳响应

**Fixed Header**:
```
Byte 0: 0xD0  (Packet Type = 13, Flags = 0x00)
```

**Remaining Length**: 0

**示例**:
```
D0 00
```

---

## DISCONNECT (客户端 → 服务端)

**用途**: 客户端主动断开连接

**Fixed Header**:
```
Byte 0: 0xE0  (Packet Type = 14, Flags = 0x00)
```

**Remaining Length**: 0

**示例**:
```
E0 00
```

---

## QoS 等级

### QoS 0 (At most once) - 至多一次

- **特点**: 消息最多传递一次，不保证送达
- **流程**: 
  ```
  发布者 → PUBLISH (QoS 0) → 订阅者
  ```
- **适用场景**: 实时数据、允许丢失的消息

---

### QoS 1 (At least once) - 至少一次

- **特点**: 消息至少传递一次，可能重复
- **流程**:
  ```
  发布者 → PUBLISH (QoS 1) → 订阅者
           ← PUBACK ←
  ```
- **超时重传**: 发布者在未收到 PUBACK 时会重传消息
- **适用场景**: 需要确保消息送达，但允许重复

---

### QoS 2 (Exactly once) - 恰好一次

- **特点**: 消息恰好传递一次，不重复
- **流程** (四次握手):
  ```
  发布者 → PUBLISH → 订阅者
           ← PUBREC ←
  发布者 → PUBREL → 订阅者
           ← PUBCOMP ←
  ```
- **存储要求**: 需要持久化中间状态（防止重启后重复投递）
- **适用场景**: 计费系统、金融交易（不允许丢失或重复）

---

## 主题通配符

### 语法规则

- **层级分隔符**: `/` (如 `home/bedroom/temperature`)
- **单层通配符**: `+` (匹配一个层级)
- **多层通配符**: `#` (匹配零个或多个层级，**只能出现在末尾**)

### 示例

| 主题过滤器 | 匹配示例 | 不匹配 |
|-----------|---------|--------|
| `home/+/temperature` | `home/bedroom/temperature` | `home/bedroom/1/temperature` |
| `home/#` | `home/bedroom/temperature` | - |
| `home/bedroom/+` | `home/bedroom/temperature` | `home/bedroom/1/temperature` |
| `+/+/+` | `home/bedroom/temperature` | `home/bedroom` |

### 系统主题 (可选)

- `$SYS/` 开头的主题保留给服务端使用
- 通配符 **不能** 匹配 `$SYS/` 开头的主题

---

## 与 v5.0 的主要差异

| 特性 | v3.1.1 | v5.0 |
|------|---------|-------|
| **Properties** | ❌ 无 | ✅ 有 (Variable Header 后添加 Properties 字段) |
| **Will Delay** | ❌ 无 | ✅ 有 (Will Delay Interval) |
| **共享订阅** | ❌ 无 | ✅ 有 (`$share/group/topic`) |
| **消息过期** | ❌ 无 | ✅ 有 (Message Expiry) |
| **主题别名** | ❌ 无 | ✅ 有 (Topic Alias) |
| **订阅标识符** | ❌ 无 | ✅ 有 (Subscription Identifier) |
| **请求/响应** | ❌ 无 | ✅ 有 (Request/Response Information) |
| **最大包大小** | ❌ 无 | ✅ 有 (Maximum Packet Size) |
| **服务器重定向** | ❌ 无 | ✅ 有 (Server Reference) |
| **错误详情** | ❌ 简单返回码 | ✅ 有 (Reason Code + Reason String) |

### 包格式差异

**v3.1.1 CONNECT**:
```
Fixed Header
Remaining Length
Protocol Name (6 bytes)
Protocol Level (1 byte)
Connect Flags (1 byte)
Keep Alive (2 bytes)
Payload (Client ID, Will, Username, Password)
```

**v5.0 CONNECT**:
```
Fixed Header
Remaining Length
Protocol Name (6 bytes)
Protocol Level (1 byte)
Connect Flags (1 byte)
Keep Alive (2 bytes)
Properties Length (1+ bytes)   ← 新增
Properties                     ← 新增
Payload (Client ID, Will, Username, Password)
```

---

## 常见返回码

### CONNACK Return Code

| 值 | 含义 |
|----|------|
| 0 | 连接已接受 |
| 1 | 不支持的协议版本 |
| 2 | 拒绝的客户端标识符 (Client ID 格式错误) |
| 3 | 服务端不可用 |
| 4 | 无效的用户名或密码 |
| 5 | 未授权 |

### SUBACK Return Code

| 值 | 含义 |
|----|------|
| 0 | QoS 0 |
| 1 | QoS 1 |
| 2 | QoS 2 |
| 0x80 | 订阅失败 |

---

## 实现注意事项

### 1. Packet ID 管理

- **范围**: 1 ~ 65535
- **QoS 0**: 不需要 Packet ID
- **QoS 1/2**: 必须包含 Packet ID
- **去重**: 同一个会话中，Packet ID 在确认前不得重复使用

### 2. Keep Alive 机制

- **客户端**: 在 1.5 倍 Keep Alive 时间内未发送任何包，必须发送 PINGREQ
- **服务端**: 在 1.5 倍 Keep Alive 时间内未收到任何包，必须断开连接
- **值 = 0**: 禁用 Keep Alive

### 3. 清理会话 (Clean Session)

- **Clean Session = 1**: 
  - 客户端不要求恢复旧会话
  - 服务端丢弃旧会话状态
  - 创建新会话
  
- **Clean Session = 0**:
  - 客户端要求恢复旧会话（如果存在）
  - 服务端必须恢复旧会话状态（订阅、离线消息）
  - 如果不存在旧会话，创建新会话

### 4. Retained 消息

- **发布时 Retain = 1**: 
  - 服务端必须存储该主题的最后一条 Retained 消息
  - 新订阅者会立即收到 Retained 消息
  
- **订阅时**:
  - 如果主题有 Retained 消息，服务端必须立即发送
  
- **清除 Retained 消息**:
  - 发布空消息 (Payload 长度为 0) 且 Retain = 1

### 5. Will Message (遗嘱消息)

- **触发条件**:
  - 客户端网络异常断开
  - 客户端未发送 DISCONNECT 就关闭连接
  
- **发送时机**: 
  - 服务端检测到连接断开后，立即发布 Will Message
  
- **QoS 和 Retain**:
  - Will Message 的 QoS 和 Retain 由 CONNECT 包中的 Will QoS 和 Will Retain 字段决定

---

## 参考资源

- **官方规范**: [OASIS MQTT v3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)
- **HiveMQ 指南**: [MQTT Essentials](https://www.hivemq.com/mqtt-essentials/)
- **Eclipse Paho**: [MQTT Resources](https://www.eclipse.org/paho/)

---

**最后更新**: 2026-06-27  
**维护者**: AI Assistant + 用户
