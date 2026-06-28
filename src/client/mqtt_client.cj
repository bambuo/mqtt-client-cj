// MQTT 客户端主类
package mqtt.client

import mqtt.protocol.*
import mqtt.transport.TcpTransport
import mqtt.transport.WsTransport
import mqtt.store.MessageStore
import mqtt.store.InflightState
import mqtt.store.InflightMessage
import std.convert.*
import std.time.*
import std.collection.*
import std.sync.*

public class MqttClient {
    private var transport: Transport
    private var options: ConnectOptions
    private let codec: ProtocolCodec      // 协议版本策略，初始化后只读
    private var isConnected: Bool = false
    private var messageStore: MessageStore  // Inflight 消息存储（QoS 1/2）
    private var topicCallbacks: Map<String, (String, Array<UInt8>) -> Unit>  // per-subscription 回调
    private var subscriptions: HashMap<String, QoS> = HashMap()  // 订阅追踪（用于重连恢复）
    private var reconnectAttempt: Int64 = 0
    private var shouldReconnect: Bool = false

    // 互斥锁：保护所有共享可变状态（messageStore / receiveBuffer /
    // topicCallbacks / subscriptions / qos2Incoming / 连接状态字段），
    // 因接收线程、KeepAlive 线程、重连线程、用户线程并发访问。
    // Mutex 默认可重入，回调内若再次加锁不会死锁。
    private let lock = Mutex()

    // 接收缓冲区（处理 TCP 分包/粘包）
    private var receiveBuffer = ArrayList<UInt8>()

    // 接收到的 QoS 2 消息（等待 PUBREL 确认），直接存储解析结果避免二次解析
    private var qos2Incoming: HashMap<Int64, PublishResult> = HashMap()

    // v5.0 Topic Alias 映射（alias → topic），用于接收侧解析
    // 当 PUBLISH 的 Topic Name 为零长时，根据 Topic Alias 属性查找真实主题
    private var topicAliasMap: HashMap<Int64, String> = HashMap()

    // 待确认订阅追踪（packetId → topic），用于 SUBACK 失败时清理
    private var pendingSubscriptions: HashMap<Int64, String> = HashMap()

    // Keep Alive 计时字段
    private var lastPingReqTime: Int64 = 0  // 上次发送 PINGREQ 的时间
    private var lastPingRespTime: Int64 = 0 // 上次收到 PINGRESP 的时间
    private var keepAliveGeneration: Int64 = 0  // 每连接递增，用于淘汰旧 KeepAlive 线程

    // CONNACK 等待状态
    private var connAckReceived: Bool = false
    private var connAckSuccess: Bool = false
    private var sessionPresent: Bool = false
    private var serverProps: MqttProperties = MqttProperties()
    private let connAckTimeoutMs: Int64 = 10000  // 10 秒超时

    // 回调函数
    public var onConnect: () -> Unit = { => }
    public var onDisconnect: () -> Unit = { => }
    public var onMessage: (String, Array<UInt8>) -> Unit = { topic, payload =>
        println("收到消息 [${topic}]: ${String.fromUtf8(payload)}")
    }
    public var onError: (String) -> Unit = { error =>
        println("错误: ${error}")
    }

    public init(options: ConnectOptions) {
        this.options = options
        if (options.useWs) {
            this.transport = WsTransport(options.host, options.port,
                useTls: options.useTls, wsPath: options.wsPath)
        } else {
            this.transport = TcpTransport(options.host, options.port,
                useTls: options.useTls, tlsConfig: options.tlsOptions)
        }
        // 根据协议版本创建对应的 Codec 策略——后续所有编解码通过多态分派
        this.codec = ProtocolCodec.create(options.protocolVersion)
        this.messageStore = MessageStore()
        this.topicCallbacks = HashMap()
    }

    // 连接到 broker
    public func connect(): Result<Unit, String> {
        println("正在连接到 ${options.host}:${options.port}...")

        // 建立 TCP 连接
        let connResult = transport.connect()
        if (connResult.isErr()) {
            return connResult
        }

        // 设置接收回调
        transport.onReceive { data =>
            this.handleReceivedData(data)
        }

        // 发送 CONNECT 包（多态：自动根据协议版本选择格式）
        let connectPacket = codec.encodeConnect(options)
        let sendResult = transport.send(connectPacket)
        if (sendResult.isErr()) {
            transport.close()
            return Err("发送 CONNECT 失败: " + sendResult.unwrapErr())
        }

        // 等待 CONNACK（带超时）
        synchronized(lock) {
            connAckReceived = false
        }
        var waited: Int64 = 0
        var received = synchronized(lock) { connAckReceived }
        while (waited < connAckTimeoutMs && !received) {
            sleep(Duration.millisecond * 100)
            waited += 100
            received = synchronized(lock) { connAckReceived }
        }

        if (!received) {
            transport.close()
            return Err("CONNACK 超时")
        }

        let success = synchronized(lock) { connAckSuccess }
        if (!success) {
            transport.close()
            return Err("连接被拒绝")
        }

        synchronized(lock) {
            isConnected = true
            keepAliveGeneration += 1
            // 连接成功后，若启用自动重连则允许后续断线时触发重连
            if (options.autoReconnect) {
                shouldReconnect = true
            }
        }

        // 启动 Keep Alive 定时器
        startKeepAlive()

        // 触发连接成功回调
        onConnect()

        return Ok(())
    }

    // 发布消息
    public func publish(topic: String, payload: String, qos!: QoS = QoS.AT_MOST_ONCE,
        retain!: Bool = false): Result<Int, String> {
        if (!synchronized(lock) { isConnected }) {
            return Err("未连接到 broker")
        }

        // 校验 topic 合法性（不允许通配符）
        if (!TopicMatcher.isValidTopic(topic: topic)) {
            return Err("非法的 topic: ${topic}（包含通配符）")
        }

        let payloadBytes = stringToUtf8(payload)

        // 生成 packetId + 编码 + 存储（共享状态变更，加锁）
        // 返回 None 表示所有 packet ID 都在 in-flight（背压）
        let prepared: ?(Int64, Array<UInt8>) = synchronized(lock) {
            // QoS 1/2 需要 packet ID
            var pidOpt: ?Int64 = None
            match (qos) {
                case AT_LEAST_ONCE => pidOpt = messageStore.generatePacketId()
                case EXACTLY_ONCE => pidOpt = messageStore.generatePacketId()
                case _ => pidOpt = Some(0)
            }
            var result: ?(Int64, Array<UInt8>) = None
            if (pidOpt.isSome()) {
                let pid = pidOpt.getOrThrow()
                let pkt = codec.encodePublish(topic, payloadBytes, qos, pid, retain, false, MqttProperties())
                // QoS 1/2：存储 inflight 消息
                match (qos) {
                    case AT_LEAST_ONCE =>
                        messageStore.store(pid, pkt, InflightState.SENT)
                    case EXACTLY_ONCE =>
                        messageStore.store(pid, pkt, InflightState.SENT)
                    case _ => ()
                }
                result = Some((pid, pkt))
            }
            result
        }

        if (prepared.isNone()) {
            return Err("无可用 packet ID（所有 ID 均在 in-flight）")
        }
        let (packetId, packet) = prepared.getOrThrow()

        let result = transport.send(packet)

        // QoS 1/2：如果发送失败，删除 inflight 消息
        if (result.isErr()) {
            synchronized(lock) {
                match (qos) {
                    case AT_LEAST_ONCE => messageStore.remove(packetId)
                    case EXACTLY_ONCE => messageStore.remove(packetId)
                    case _ => ()
                }
            }
        }

        return result
    }

    // 发布字节消息
    public func publishBytes(topic: String, payload: Array<UInt8>, qos!: QoS = QoS.AT_MOST_ONCE,
        retain!: Bool = false): Result<Int, String> {
        if (!synchronized(lock) { isConnected }) {
            return Err("未连接到 broker")
        }

        // 校验 topic 合法性（不允许通配符）
        if (!TopicMatcher.isValidTopic(topic: topic)) {
            return Err("非法的 topic: ${topic}（包含通配符）")
        }

        // 生成 packetId + 编码 + 存储（共享状态变更，加锁）
        // 返回 None 表示所有 packet ID 都在 in-flight（背压）
        let prepared: ?(Int64, Array<UInt8>) = synchronized(lock) {
            // QoS 1/2 需要 packet ID
            var pidOpt: ?Int64 = None
            match (qos) {
                case AT_LEAST_ONCE => pidOpt = messageStore.generatePacketId()
                case EXACTLY_ONCE => pidOpt = messageStore.generatePacketId()
                case _ => pidOpt = Some(0)
            }
            var result: ?(Int64, Array<UInt8>) = None
            if (pidOpt.isSome()) {
                let pid = pidOpt.getOrThrow()
                let pkt = codec.encodePublish(topic, payload, qos, pid, retain, false, MqttProperties())
                // QoS 1/2：存储 inflight 消息
                match (qos) {
                    case AT_LEAST_ONCE =>
                        messageStore.store(pid, pkt, InflightState.SENT)
                    case EXACTLY_ONCE =>
                        messageStore.store(pid, pkt, InflightState.SENT)
                    case _ => ()
                }
                result = Some((pid, pkt))
            }
            result
        }

        if (prepared.isNone()) {
            return Err("无可用 packet ID（所有 ID 均在 in-flight）")
        }
        let (packetId, packet) = prepared.getOrThrow()

        let result = transport.send(packet)

        // QoS 1/2：如果发送失败，删除 inflight 消息
        if (result.isErr()) {
            synchronized(lock) {
                match (qos) {
                    case AT_LEAST_ONCE => messageStore.remove(packetId)
                    case EXACTLY_ONCE => messageStore.remove(packetId)
                    case _ => ()
                }
            }
        }

        return result
    }

    // 订阅主题（支持 per-subscription 回调 + 通配符）
    public func subscribe(
        topic: String,
        qos!: QoS = QoS.AT_MOST_ONCE,
        callback!: Option<(String, Array<UInt8>) -> Unit> = None
    ): Result<Unit, String> {
        if (!synchronized(lock) { isConnected }) {
            return Err("未连接到 broker")
        }

        // 验证 topic filter
        if (!TopicMatcher.isValidTopicFilter(topicFilter: topic)) {
            return Err("非法的 topic filter: ${topic}")
        }

        // 存储回调 / 订阅 + 编码（共享状态变更，加锁）
        // 返回 None 表示所有 packet ID 都在 in-flight
        let packetOpt: ?Array<UInt8> = synchronized(lock) {
            // 存储回调（如果提供）
            if (callback.isSome()) {
                topicCallbacks.add(topic, callback.getOrThrow())
            }
            // 记录订阅（用于重连恢复）
            subscriptions.add(topic, qos)

            let pidOpt = messageStore.generatePacketId()
            if (pidOpt.isNone()) {
                None
            } else {
                let packetId = pidOpt.getOrThrow()
                // 追踪待确认订阅，供 SUBACK 失败时清理
                pendingSubscriptions.add(packetId, topic)
                let subs = [Subscription(topic, qos: qos)]
                Some(codec.encodeSubscribe(subs, packetId))
            }
        }

        if (packetOpt.isNone()) {
            return Err("无可用 packet ID（所有 ID 均在 in-flight）")
        }
        let packet = packetOpt.getOrThrow()

        let result = transport.send(packet)
        if (result.isErr()) {
            // 如果发送失败，删除已存储的回调和订阅记录
            synchronized(lock) {
                if (callback.isSome()) {
                    topicCallbacks.remove(topic)
                }
                subscriptions.remove(topic)
            }
            return Err(result.unwrapErr())
        }

        return Ok(())
    }

    // 取消订阅
    public func unsubscribe(topic: String): Result<Unit, String> {
        if (!synchronized(lock) { isConnected }) {
            return Err("未连接到 broker")
        }

        let packetOpt: ?Array<UInt8> = synchronized(lock) {
            let pidOpt = messageStore.generatePacketId()
            if (pidOpt.isNone()) {
                None
            } else {
                let packetId = pidOpt.getOrThrow()
                let topics = [topic]
                Some(codec.encodeUnsubscribe(topics, packetId))
            }
        }

        if (packetOpt.isNone()) {
            return Err("无可用 packet ID（所有 ID 均在 in-flight）")
        }
        let packet = packetOpt.getOrThrow()

        let result = transport.send(packet)
        if (result.isOk()) {
            synchronized(lock) {
                topicCallbacks.remove(topic)
                subscriptions.remove(topic)
            }
        }

        return match (result) {
            case Ok(_) => Ok(())
            case Err(e) => Err(e)
        }
    }

    // 断开连接
    public func disconnect() {
        let wasConnected = synchronized(lock) {
            if (isConnected) {
                shouldReconnect = false
                reconnectAttempt = 0
                isConnected = false
                true
            } else {
                false
            }
        }
        if (wasConnected) {
            let disconnectPacket = codec.encodeDisconnect(ReasonCode.Success, MqttProperties())
            try {
                transport.send(disconnectPacket)
            } catch (e: Exception) {
                // 忽略发送错误
            }

            stopKeepAlive()
            transport.close()
            onDisconnect()
        }
    }

    // 处理连接丢失（触发自动重连）
    private func handleConnectionLost(): Unit {
        let needReconnect = synchronized(lock) {
            isConnected = false
            // 仅当用户未主动断开（shouldReconnect 仍为 true）且开启自动重连时才启动重连
            shouldReconnect && options.autoReconnect
        }
        onDisconnect()  // 始终通知断开（锁外，避免持锁调用用户回调）
        if (needReconnect) {
            startReconnect()
        }
    }

    // 自动重连（带并发防护，确保只有一个重连线程运行）
    private func startReconnect(): Unit {
        // check-then-set 在锁内原子完成，避免多个重连线程
        let start = synchronized(lock) {
            if (!options.autoReconnect || options.maxReconnectAttempts <= 0) {
                false
            } else if (shouldReconnect) {
                false  // 已有重连线程在运行
            } else {
                shouldReconnect = true
                true
            }
        }
        if (!start) {
            return
        }
        spawn {
            while (true) {
                // 锁内快照循环条件
                let (keepGoing, attempt, baseDelay, maxAttempts) = synchronized(lock) {
                    (shouldReconnect && reconnectAttempt < options.maxReconnectAttempts,
                        reconnectAttempt + 1,
                        options.reconnectBaseDelayMs,
                        options.maxReconnectAttempts)
                }
                if (!keepGoing) { break }
                synchronized(lock) {
                    reconnectAttempt = attempt
                }

                var delayMs = baseDelay * attempt
                if (delayMs > 30000) { delayMs = 30000 }
                println("重连尝试 ${attempt}/${maxAttempts} (等待 ${delayMs}ms)...")
                sleep(Duration.millisecond * delayMs)

                var abort = false
                synchronized(lock) {
                    if (!shouldReconnect) { abort = true }
                }
                if (abort) { break }

                let r = this.connect()
                if (r.isOk()) {
                    // 锁内读取 sessionPresent 与订阅快照
                    let (sessionPres, subs) = synchronized(lock) {
                        let snapshot = ArrayList<(String, QoS)>()
                        for ((topic, qos) in subscriptions) {
                            snapshot.add((topic, qos))
                        }
                        (this.sessionPresent, snapshot)
                    }
                    println("重连成功 (session present=${sessionPres})")
                    if (!sessionPres) {
                        // 重新订阅（仅当 broker 未恢复会话时）
                        for ((topic, qos) in subs) {
                            let pidOpt = synchronized(lock) { messageStore.generatePacketId() }
                            if (pidOpt.isNone()) {
                                onError("重连后无法分配 packet ID，跳过订阅 ${topic}")
                                continue
                            }
                            let subsArr = [Subscription(topic, qos: qos)]
                            let pkt = codec.encodeSubscribe(subsArr, pidOpt.getOrThrow())
                            let _ = transport.send(pkt)
                        }
                    }

                    // 重连后重传 in-flight QoS1/2 消息（MQTT §3.1.2-5 / §3.1.2-11）
                    // 先做快照，锁外发送，再锁内更新时间戳
                    let inflights = synchronized(lock) {
                        let snapshot = ArrayList<(Int64, InflightState, Array<UInt8>)>()  // (packetId, state, 现有 packetData)
                        // 遍历所有 in-flight 条目并编码待发送字节
                        // （messageStore 内部迭代需直接访问，此处用快照方式）
                        let timeoutMsgs = messageStore.getTimeoutMessages(timeout: 0)
                        for (inf in timeoutMsgs) {
                            let retryData = match (inf.state) {
                                case InflightState.WAIT_PUBREL =>
                                    codec.encodePubRel(inf.packetId)
                                case _ =>
                                    setDupFlag(inf.packetData)
                            }
                            snapshot.add((inf.packetId, inf.state, retryData))
                        }
                        snapshot
                    }
                    for ((pid, _, data) in inflights) {
                        let _ = transport.send(data)
                        println("重连后重传 inflight (packetId=${pid})")
                    }
                    synchronized(lock) {
                        let timeoutMsgs = messageStore.getTimeoutMessages(timeout: 0)
                        for (inf in timeoutMsgs) {
                            inf.updateTimestamp()
                        }
                    }

                    synchronized(lock) {
                        shouldReconnect = false
                        reconnectAttempt = 0
                    }
                    return
                } else {
                    println("重连失败: ${r.unwrapErr()}")
                }

                var reachedMax = false
                synchronized(lock) {
                    if (reconnectAttempt >= options.maxReconnectAttempts) { reachedMax = true }
                }
                if (reachedMax) {
                    onError("重连次数已达上限")
                    synchronized(lock) {
                        shouldReconnect = false
                    }
                    break
                }
            }
            synchronized(lock) {
                shouldReconnect = false
            }
        }
    }

    // 处理接收到的数据（支持 TCP 分包/粘包）
    private func handleReceivedData(data: Array<UInt8>) {
        // 从缓冲区中提取所有可解析的完整包（加锁保护 receiveBuffer）
        // 同时收集解析错误，锁外上报避免持锁调用用户回调
        var parseErrors = ArrayList<String>()
        let packets = synchronized(lock) {
            // 将新数据加入缓冲区
            for (b in data) { receiveBuffer.add(b) }

            let ready = ArrayList<MqttPacket>()

            // 循环从缓冲区中提取完整包
            while (true) {
                // 尝试计算一个完整包的大小
                let bufArray = toArray(receiveBuffer)
                let sizeOpt = MqttDecoder.getPacketSize(bufArray)
                if (sizeOpt.isNone()) {
                    break  // 数据不完整，等待更多数据
                }

                let totalSize = sizeOpt.getOrThrow()
                // 截取完整包的数据
                let packetData = Array<UInt8>(totalSize, repeat: 0)
                for (i in 0..totalSize) { packetData[Int64(i)] = bufArray[Int64(i)] }

                let packetOpt = MqttDecoder.decode(packetData)
                if (packetOpt.isNone()) {
                    // 解析失败：跳过 1 字节后继续（防止恶意数据卡死），而非清空全部
                    parseErrors.add("无法解析数据包，跳过 1 字节")
                    if (receiveBuffer.size > 1) {
                        var newBuf = ArrayList<UInt8>()
                        for (i in 1..receiveBuffer.size) { newBuf.add(receiveBuffer[Int64(i)]) }
                        receiveBuffer = newBuf
                        continue
                    } else {
                        receiveBuffer.clear()
                        break
                    }
                }

                // 从缓冲区移除已消费的字节
                var newBuffer = ArrayList<UInt8>()
                for (i in totalSize..receiveBuffer.size) {
                    newBuffer.add(receiveBuffer[Int64(i)])
                }
                receiveBuffer = newBuffer

                ready.add(packetOpt.getOrThrow())
            }
            ready
        }

        // 锁外上报解析错误（避免持锁调用用户回调）
        for (msg in parseErrors) {
            onError(msg)
        }

        // 在锁外处理每个包（处理过程内部按需加锁），避免长时间持锁
        for (packet in packets) {
            processPacket(packet)
        }
    }

    // 处理已解码的 MQTT 包
    private func processPacket(packet: MqttPacket) {
        // 异常包（畸形/截断）不应安静杀死接收线程
        try {
            match (packet.packetType) {
            case CONNACK =>
                handleConnAck(packet)
            case PUBLISH =>
                handlePublish(packet)
            case PUBACK =>
                handlePubAck(packet)
            case PUBREC =>
                handlePubRec(packet)
            case PUBREL =>
                handlePubRel(packet)
            case PUBCOMP =>
                handlePubComp(packet)
            case SUBACK =>
                handleSubAck(packet)
            case UNSUBACK =>
                handleUnsubAck(packet)
            case AUTH =>
                handleAuth(packet)
            case PINGRESP =>
                synchronized(lock) {
                    lastPingRespTime = getCurrentTimeMillis()
                }
                println("PINGRESP 已收到")
            case DISCONNECT =>
                let discResult = codec.parseDisconnect(packet)
                println("DISCONNECT (reasonCode=${discResult.reasonCode.toInt()})")
                if (discResult.reasonCode.toInt() != 0) {
                    onError("服务端断开连接: reasonCode=${discResult.reasonCode.toInt()}")
                }
                handleConnectionLost()
            case _ =>
                println("收到未处理的包类型: type=${packet.packetType.toInt()}")
            }
        } catch (e: Exception) {
            onError("协议解析异常: ${e.message}（跳过该包）")
        }
    }

    // 处理 PUBACK (QoS 1 确认)
    private func handlePubAck(packet: MqttPacket): Unit {
        let result = codec.parsePubAck(packet)
        println("PUBACK (packetId=${result.packetId}, reasonCode=${result.reasonCode.toInt()})")

        // v5.0: 如果 reasonCode >= 0x80，表示失败，消息需要重传（不删除 inflight）
        if (!result.success) {
            println("PUBACK 失败 (reasonCode=${result.reasonCode.toInt()})，消息将重传")
            return
        }

        // 从 inflight 消息中删除（共享状态变更，加锁）
        let removed = synchronized(lock) {
            messageStore.remove(result.packetId)
        }

        if (removed.isSome()) {
            println("消息已确认 (QoS 1, packetId=${result.packetId})")
        } else {
            println("收到未知的 PUBACK (packetId=${result.packetId})")
        }
    }

    // 处理 PUBREC (QoS 2 第一步响应)
    private func handlePubRec(packet: MqttPacket): Unit {
        let result = codec.parsePubRec(packet)
        println("PUBREC (packetId=${result.packetId}, reasonCode=${result.reasonCode.toInt()})")

        // v5.0: 若 PUBREC 返回失败 (reasonCode >= 0x80)，不回 PUBREL，消息保持 in-flight 等待重传或上层处理
        if (!result.success) {
            println("PUBREC 失败 (reasonCode=${result.reasonCode.toInt()})，不回 PUBREL")
            return
        }

        // 更新 inflight 状态为 WAIT_PUBREL（共享状态变更，加锁）
        let updated = synchronized(lock) {
            messageStore.updateState(result.packetId, InflightState.WAIT_PUBREL)
        }

        if (updated) {
            // 发送 PUBREL
            let pubRelPacket = codec.encodePubRel(result.packetId)
            let sendResult = transport.send(pubRelPacket)

            if (sendResult.isOk()) {
                println("PUBREL 已发送 (packetId=${result.packetId})")
            } else {
                onError("发送 PUBREL 失败: ${sendResult.unwrapErr()}")
            }
        } else {
            println("收到未知的 PUBREC (packetId=${result.packetId})")
        }
    }

    // 处理 PUBREL (QoS 2 第三步 — 收到 PUBREL 后交付消息并发送 PUBCOMP)
    private func handlePubRel(packet: MqttPacket): Unit {
        let result = codec.parsePubRel(packet)
        println("PUBREL (packetId=${result.packetId}, reasonCode=${result.reasonCode.toInt()})")

        // v5.0: 若 PUBREL 携带失败 Reason Code，不交付消息（MQTT-4.3.3-6）
        // 仍发送 PUBCOMP 以终止 QoS2 流程
        if (!result.success) {
            println("PUBREL 失败 (reasonCode=${result.reasonCode.toInt()})，不交付消息")
            onError("QoS 2 消息交付失败 (packetId=${result.packetId}, reasonCode=${result.reasonCode.toInt()})")
        } else {
            // 查找存储的 QoS 2 消息，并收集匹配的回调（共享状态变更，加锁）
            let pubResultOpt = synchronized(lock) {
                qos2Incoming.remove(result.packetId)
            }

            if (pubResultOpt.isSome()) {
                let pubResult = pubResultOpt.getOrThrow()
                let callbacks = synchronized(lock) {
                    let matched = ArrayList<(String, Array<UInt8>) -> Unit>()
                    for ((topicFilter, callbackFn) in topicCallbacks) {
                        if (TopicMatcher.matches(topic: pubResult.topic, topicFilter: topicFilter)) {
                            matched.add(callbackFn)
                        }
                    }
                    matched
                }
                if (callbacks.size == 0) {
                    onMessage(pubResult.topic, pubResult.payload)
                } else {
                    for (cb in callbacks) {
                        cb(pubResult.topic, pubResult.payload)
                    }
                }
            }
        }

        // 发送 PUBCOMP
        let pubCompPacket = codec.encodePubComp(result.packetId)
        let sendResult = transport.send(pubCompPacket)

        if (sendResult.isOk()) {
            println("PUBCOMP 已发送 (packetId=${result.packetId})")
        } else {
            onError("发送 PUBCOMP 失败: ${sendResult.unwrapErr()}")
        }
    }

    // 处理 PUBCOMP (QoS 2 完成)
    private func handlePubComp(packet: MqttPacket): Unit {
        let result = codec.parsePubComp(packet)
        println("PUBCOMP (packetId=${result.packetId}, reasonCode=${result.reasonCode.toInt()})")

        // v5.0: PUBCOMP 可能携带失败 Reason Code
        if (!result.success) {
            println("PUBCOMP 失败 (reasonCode=${result.reasonCode.toInt()})")
            onError("QoS 2 流程异常终止 (packetId=${result.packetId}, reasonCode=${result.reasonCode.toInt()})")
        }

        // 从 inflight 消息中删除（共享状态变更，加锁）
        let removed = synchronized(lock) {
            messageStore.remove(result.packetId)
        }

        if (removed.isSome()) {
            println("消息已确认 (QoS 2 完成, packetId=${result.packetId})")
        } else {
            println("收到未知的 PUBCOMP (packetId=${result.packetId})")
        }
    }

    // 处理 CONNACK
    private func handleConnAck(packet: MqttPacket) {
        let result = codec.parseConnAck(packet)

        // 更新服务端属性 / 连接状态（共享状态变更，加锁）
        // 回调执行放在锁外，避免持锁调用用户代码
        var connError: ?String = None
        synchronized(lock) {
            // 解析 CONNACK Properties（v5.0 专用，v3.1.1 返回空对象）
            if (!result.properties.isEmpty()) {
                this.serverProps = result.properties
                // 如果服务器指定了 keepAlive，覆盖本地值
                if (this.serverProps.serverKeepAlive.isSome()) {
                    let serverKeepAlive = this.serverProps.serverKeepAlive.getOrThrow()
                    println("服务端指定 KeepAlive: ${serverKeepAlive} 秒")
                    if (serverKeepAlive > 0) {
                        this.options.keepAlive = serverKeepAlive
                    }
                }
            }

            match (result.returnCode) {
                case ACCEPTED =>
                    this.sessionPresent = result.sessionPresent
                    println("连接成功! (session present=${result.sessionPresent})")
                    connAckReceived = true
                    connAckSuccess = true
                case _ =>
                    connAckReceived = true
                    connAckSuccess = false
                    let errorCode = match (result.returnCode) {
                        case WRONG_VERSION => "不支持的协议版本"
                        case INVALID_ID => "拒绝的客户端标识符"
                        case SERVER_UNAVAILABLE => "服务端不可用"
                        case BAD_CREDENTIALS => "无效的用户名或密码"
                        case NOT_AUTHORIZED => "未授权"
                        case _ => "未知错误"
                    }
                    println("连接被拒绝: ${errorCode}")
                    connError = Some(errorCode)
            }
        }
        // 锁外调用用户回调
        if (connError.isSome()) {
            onError("连接被拒绝: ${connError.getOrThrow()}")
        }
    }

    // 处理 SUBACK
    private func handleSubAck(packet: MqttPacket): Unit {
        let result = codec.parseSubAck(packet)
        println("SUBACK 已确认 (packetId=${result.packetId})")
        for (qos in result.grantedQos) {
            match (qos) {
                case -1 =>
                    println("  主题订阅失败")
                    // 清理失败订阅记录（避免重连时重复发送、避免残留回调）
                    synchronized(lock) {
                        let failedTopic = pendingSubscriptions.remove(result.packetId)
                        if (failedTopic.isSome()) {
                            topicCallbacks.remove(failedTopic.getOrThrow())
                            subscriptions.remove(failedTopic.getOrThrow())
                        }
                    }
                case 0 => println("  主题订阅: QoS 0")
                case 1 => println("  主题订阅: QoS 1")
                case 2 => println("  主题订阅: QoS 2")
                case _ => println("  未知返回码: ${qos}")
            }
        }
        // 无论成功失败，清理 pending 追踪
        synchronized(lock) {
            pendingSubscriptions.remove(result.packetId)
        }
    }

    // 处理 UNSUBACK
    private func handleUnsubAck(packet: MqttPacket): Unit {
        let result = codec.parseUnsubAck(packet)
        println("UNSUBACK 已确认 (packetId=${result.packetId})")
    }

    // 处理 AUTH (v5.0 only)
    private func handleAuth(packet: MqttPacket): Unit {
        let result = codec.parseAuth(packet)
        println("AUTH 收到 (reasonCode=${result.reasonCode.toInt()})")
        if (result.reasonCode.toInt() == 24) {  // ContinueAuthentication
            let authResp = codec.encodeAuth(ReasonCode.ContinueAuthentication, MqttProperties())
            let _ = transport.send(authResp)
        }
    }

    // 处理 PUBLISH
    private func handlePublish(packet: MqttPacket) {
        let result = codec.parsePublish(packet)

        // v5.0 Topic Alias 解析（MQTT-5.0 §3.3.4）
        let resolvedTopic = synchronized(lock) {
            if (result.topic.isEmpty()) {
                // 零长 Topic Name → 必须通过 Topic Alias 还原
                if (result.properties.topicAlias.isSome()) {
                    let alias = result.properties.topicAlias.getOrThrow()
                    let realTopic = topicAliasMap.get(alias)
                    if (realTopic.isSome()) {
                        realTopic.getOrThrow()
                    } else {
                        ""  // 别名未注册（协议错误，broker 不应发送此情况）
                    }
                } else {
                    ""  // 既无 Topic Name 也无 Alias（协议错误）
                }
            } else {
                // 非零 Topic Name → 如果带 Topic Alias 则注册/更新映射
                if (result.properties.topicAlias.isSome()) {
                    let alias = result.properties.topicAlias.getOrThrow()
                    topicAliasMap.add(alias, result.topic)
                }
                result.topic
            }
        }

        if (resolvedTopic.isEmpty()) {
            onError("无法解析 PUBLISH Topic: 零长 Topic Name 且无有效 Topic Alias")
            return
        }

        // 收集匹配的回调（仅读 topicCallbacks，短时加锁）；回调执行放在锁外
        let effectiveTopic = resolvedTopic
        let callbacks = synchronized(lock) {
            let matched = ArrayList<(String, Array<UInt8>) -> Unit>()
            for ((topicFilter, callbackFn) in topicCallbacks) {
                if (TopicMatcher.matches(topic: effectiveTopic, topicFilter: topicFilter)) {
                    matched.add(callbackFn)
                }
            }
            matched
        }

        // 交付消息给回调（QoS 0/1 在此交付；QoS 2 推迟至 PUBREL 到达后交付，MQTT-4.3.3）
        match (result.qos) {
            case EXACTLY_ONCE => ()  // 暂不交付，在 handlePubRel 中交付
            case _ =>
                if (callbacks.size == 0) {
                    onMessage(effectiveTopic, result.payload)
                } else {
                    for (cb in callbacks) {
                        cb(effectiveTopic, result.payload)
                    }
                }
        }

        // QoS 1 和 QoS 2: 发送确认
        match (result.qos) {
            case AT_LEAST_ONCE =>
                let pubAckPacket = codec.encodePubAck(result.packetId)
                let pubResult = transport.send(pubAckPacket)
                if (pubResult.isErr()) {
                    onError("发送 PUBACK 失败: ${pubResult.unwrapErr()}")
                }
            case EXACTLY_ONCE =>
                // 检查是否已收到过此 Packet ID（重复 QoS 2 PUBLISH 应忽略，MQTT-3.3.2-3）
                let alreadySeen = synchronized(lock) {
                    if (qos2Incoming.contains(result.packetId)) {
                        true
                    } else {
                        // 存储收到的 QoS 2 消息解析结果，供后续 PUBREL 交付回调使用
                        qos2Incoming.add(result.packetId, result)
                        false
                    }
                }

                if (alreadySeen) {
                    println("忽略重复的 QoS 2 PUBLISH (packetId=${result.packetId})")
                    // 仍然需要发送 PUBREC 以完成协议流程，但不重复交付
                    let pubRecPacket = codec.encodePubRec(result.packetId)
                    let pubRecResult = transport.send(pubRecPacket)
                    if (pubRecResult.isErr()) {
                        onError("发送 PUBREC 失败: ${pubRecResult.unwrapErr()}")
                    }
                } else {
                    // 发送 PUBREC
                    let pubRecPacket = codec.encodePubRec(result.packetId)
                    let pubRecResult = transport.send(pubRecPacket)
                    if (pubRecResult.isErr()) {
                        onError("发送 PUBREC 失败: ${pubRecResult.unwrapErr()}")
                    } else {
                        println("PUBREC 已发送 (packetId=${result.packetId})")
                    }
                }
            case _ => ()
        }
    }

    // 启动 Keep Alive
    private func startKeepAlive() {
        if (options.keepAlive > 0) {
            println("Keep Alive 已启用: ${options.keepAlive} 秒")

            // 初始化计时
            let currentTime = getCurrentTimeMillis()
            synchronized(lock) {
                lastPingReqTime = currentTime
                lastPingRespTime = currentTime
            }

            // 捕获当前连接代数，用于淘汰旧线程
            let myGen = synchronized(lock) { keepAliveGeneration }

            // 启动后台线程
            spawn {
                this.keepAliveLoop(myGen)
            }
        }
    }

    // Keep Alive 后台循环
    private func keepAliveLoop(myGen: Int64): Unit {
        while (true) {
            try {
                // 读共享状态 + 检查代数（若连接重连则退出旧循环）
                let (stillConnected, currentGen) = synchronized(lock) {
                    (isConnected, keepAliveGeneration)
                }
                if (!stillConnected || currentGen != myGen) {
                    break
                }

                let (currentTime, keepAliveMs, lastReq, lastResp) = synchronized(lock) {
                    (getCurrentTimeMillis(), Int64(options.keepAlive) * 1000,
                        lastPingReqTime, lastPingRespTime)
                }

                // 检查是否需要发送 PINGREQ (每 keepAlive/2 秒，但不少于 1 秒间隔)
                var pingInterval = keepAliveMs / 2
                if (pingInterval < 1000 && keepAliveMs > 0) { pingInterval = 1000 }
                if (currentTime - lastReq > pingInterval) {
                    sendPingReq()
                }

                // 检查 PINGRESP 超时 (1.5 倍 keepAlive)
                if (currentTime - lastResp > keepAliveMs * 3 / 2) {
                    println("Keep Alive 超时，连接可能已断开")
                    onError("Keep Alive 超时")
                    handleConnectionLost()
                    break
                }

                // QoS 1/2 超时重传：锁内快照决策 + 移除超限消息，锁外执行发送与上报
                let maxRetries: Int64 = 3
                let (retryPlan, droppedPids) = synchronized(lock) {
                    let plan = ArrayList<(InflightMessage, Array<UInt8>)>()  // (inflight, 待发送字节)
                    let dropped = ArrayList<Int64>()
                    for (inflight in messageStore.getTimeoutMessages()) {
                        if (inflight.retryCount >= maxRetries) {
                            dropped.add(inflight.packetId)
                            messageStore.remove(inflight.packetId)
                        } else {
                            // 按当前状态重传：
                            // - WAIT_PUBREL（已收到 PUBREC）：重传 PUBREL（不可带 DUP，规范要求）
                            // - SENT（仍等 PUBACK/PUBREC）：重传原 PUBLISH 并置 DUP 标志
                            let data = match (inflight.state) {
                                case WAIT_PUBREL =>
                                    codec.encodePubRel(inflight.packetId)
                                case _ =>
                                    setDupFlag(inflight.packetData)
                            }
                            plan.add((inflight, data))
                        }
                    }
                    (plan, dropped)
                }

                // 锁外上报超限消息
                for (pid in droppedPids) {
                    onError("消息重传超限 (packetId=${pid})")
                }

                // 锁外执行重传，记录成功项
                var retried = ArrayList<InflightMessage>()
                for ((inflight, data) in retryPlan) {
                    let sendResult = transport.send(data)
                    if (sendResult.isOk()) {
                        retried.add(inflight)
                        println("消息重传 (packetId=${inflight.packetId}, retry=${inflight.retryCount})")
                    }
                }

                // 锁内更新时间戳
                synchronized(lock) {
                    for (inflight in retried) {
                        inflight.updateTimestamp()
                    }
                }

                // 休眠 1 秒后继续检查
                sleep(Duration.millisecond * 1000)

            } catch (e: Exception) {
                println("Keep Alive 线程错误: ${e.message}")
            }
        }
    }

    // 发送 PINGREQ
    private func sendPingReq(): Unit {
        let pingPacket = codec.encodePingReq()
        let result = transport.send(pingPacket)

        if (result.isOk()) {
            synchronized(lock) {
                lastPingReqTime = getCurrentTimeMillis()
            }
            println("📨 PINGREQ 已发送")
        } else {
            println("发送 PINGREQ 失败: ${result.unwrapErr()}")
        }
    }

    // 停止 Keep Alive
    private func stopKeepAlive() {
        // 线程会在 isConnected = false 时自动退出
        println("Keep Alive 已停止")
    }

    // 检查连接状态
    public func connected(): Bool {
        let state = synchronized(lock) {
            isConnected
        }
        return state && transport.isConnected()
    }

    // 获取当前时间（毫秒）
    private func getCurrentTimeMillis(): Int64 {
        MonoTimeSample.refresh()
        return MonoTimeSample.elapsedMs
    }
}

// 单调时钟辅助
private class MonoTimeSample {
    private static let start = MonoTime.now()
    public static var elapsedMs: Int64 = 0

    public static func refresh(): Unit {
        let now = MonoTime.now()
        elapsedMs = (now - start).toMilliseconds()
    }
}

// 字符串转 UTF-8 字节数组（支持全 Unicode 范围）
private func stringToUtf8(s: String): Array<UInt8> {
    let result = ArrayList<UInt8>()
    for (c in s) {
        let code = UInt32(c)
        if (code < UInt32(0x80)) {
            result.add(UInt8(code))
        } else if (code < UInt32(0x800)) {
            result.add(UInt8(0xC0 | (code >> 6)))
            result.add(UInt8(0x80 | (code & 0x3F)))
        } else if (code < UInt32(0x10000)) {
            result.add(UInt8(0xE0 | (code >> 12)))
            result.add(UInt8(0x80 | ((code >> 6) & 0x3F)))
            result.add(UInt8(0x80 | (code & 0x3F)))
        } else {
            result.add(UInt8(0xF0 | (code >> 18)))
            result.add(UInt8(0x80 | ((code >> 12) & 0x3F)))
            result.add(UInt8(0x80 | ((code >> 6) & 0x3F)))
            result.add(UInt8(0x80 | (code & 0x3F)))
        }
    }
    return toArray(result)
}

// 为 PUBLISH 重传数据置 DUP 标志（bit 3）
// 仅适用于 PUBLISH 固定头（0x3X）；其余包类型不应调用
private func setDupFlag(rawPacket: Array<UInt8>): Array<UInt8> {
    if (rawPacket.size == 0) {
        return rawPacket
    }
    let retryData = Array<UInt8>(rawPacket.size, repeat: 0)
    retryData[0] = rawPacket[0] | UInt8(0x08)
    for (i in 1..rawPacket.size) {
        retryData[Int64(i)] = rawPacket[Int64(i)]
    }
    return retryData
}

// ArrayList<UInt8> 转 Array<UInt8>
private func toArray(list: ArrayList<UInt8>): Array<UInt8> {
    let arr = Array<UInt8>(list.size, repeat: UInt8(0))
    for (i in 0..list.size) {
        arr[Int64(i)] = list[Int64(i)]
    }
    return arr
}
