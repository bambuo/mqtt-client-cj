// MQTT 客户端主类
package mqtt.client

import mqtt.protocol.*
import mqtt.transport.TcpTransport
import mqtt.transport.WsTransport
import mqtt.store.MessageStore
import mqtt.store.InflightState
import std.convert.*
import std.time.*
import std.collection.*

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

    // 接收到的 QoS 2 消息（等待 PUBREL 确认），直接存储解析结果避免二次解析
    private var qos2Incoming: HashMap<Int64, PublishResult> = HashMap()

    // Keep Alive 计时字段
    private var lastPingReqTime: Int64 = 0  // 上次发送 PINGREQ 的时间
    private var lastPingRespTime: Int64 = 0 // 上次收到 PINGRESP 的时间

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
        connAckReceived = false
        var waited: Int64 = 0
        while (waited < connAckTimeoutMs && !connAckReceived) {
            sleep(Duration.millisecond * 100)
            waited += 100
        }

        if (!connAckReceived) {
            transport.close()
            return Err("CONNACK 超时")
        }

        if (!connAckSuccess) {
            transport.close()
            return Err("连接被拒绝")
        }

        isConnected = true

        // 启动 Keep Alive 定时器
        startKeepAlive()

        // 触发连接成功回调
        onConnect()

        return Ok(())
    }

    // 发布消息
    public func publish(topic: String, payload: String, qos!: QoS = QoS.AT_MOST_ONCE): Result<Int, String> {
        if (!isConnected) {
            return Err("未连接到 broker")
        }

        let payloadBytes = stringToUtf8(payload)

        // QoS 1/2 需要 packet ID
        var packetId: Int64 = 0
        match (qos) {
            case AT_LEAST_ONCE => packetId = messageStore.generatePacketId()
            case EXACTLY_ONCE => packetId = messageStore.generatePacketId()
            case _ => ()
        }

        let packet = codec.encodePublish(topic, payloadBytes, qos, packetId, false, false, MqttProperties())

        // QoS 1/2：存储 inflight 消息
        match (qos) {
            case AT_LEAST_ONCE =>
                messageStore.store(packetId, packet, InflightState.SENT)
            case EXACTLY_ONCE =>
                messageStore.store(packetId, packet, InflightState.SENT)
            case _ => ()
        }

        let result = transport.send(packet)

        // QoS 1/2：如果发送失败，删除 inflight 消息
        if (result.isErr()) {
            match (qos) {
                case AT_LEAST_ONCE => messageStore.remove(packetId)
                case EXACTLY_ONCE => messageStore.remove(packetId)
                case _ => ()
            }
        }

        return result
    }

    // 发布字节消息
    public func publishBytes(topic: String, payload: Array<UInt8>, qos!: QoS = QoS.AT_MOST_ONCE): Result<Int, String> {
        if (!isConnected) {
            return Err("未连接到 broker")
        }

        // QoS 1/2 需要 packet ID
        var packetId: Int64 = 0
        match (qos) {
            case AT_LEAST_ONCE => packetId = messageStore.generatePacketId()
            case EXACTLY_ONCE => packetId = messageStore.generatePacketId()
            case _ => ()
        }

        let packet = codec.encodePublish(topic, payload, qos, packetId, false, false, MqttProperties())

        // QoS 1/2：存储 inflight 消息
        match (qos) {
            case AT_LEAST_ONCE =>
                messageStore.store(packetId, packet, InflightState.SENT)
            case _ => ()
        }

        let result = transport.send(packet)

        // QoS 1/2：如果发送失败，删除 inflight 消息
        if (result.isErr()) {
            match (qos) {
                case AT_LEAST_ONCE => messageStore.remove(packetId)
                case _ => ()
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
        if (!isConnected) {
            return Err("未连接到 broker")
        }

        // 验证 topic filter
        if (!TopicMatcher.isValidTopicFilter(topicFilter: topic)) {
            return Err("非法的 topic filter: ${topic}")
        }

        // 存储回调（如果提供）
        if (callback.isSome()) {
            topicCallbacks.add(topic, callback.getOrThrow())
        }
        // 记录订阅（用于重连恢复）
        subscriptions.add(topic, qos)

        let subs = [Subscription(topic, qos: qos)]
        let packet = codec.encodeSubscribe(subs, 1)

        let result = transport.send(packet)
        if (result.isErr()) {
            // 如果发送失败，删除已存储的回调
            if (callback.isSome()) {
                topicCallbacks.remove(topic)
            }
            return Err(result.unwrapErr())
        }

        return Ok(())
    }

    // 取消订阅
    public func unsubscribe(topic: String): Result<Unit, String> {
        if (!isConnected) {
            return Err("未连接到 broker")
        }

        let packetId = messageStore.generatePacketId()
        let topics = [topic]
        let packet = codec.encodeUnsubscribe(topics, packetId)

        let result = transport.send(packet)
        if (result.isOk()) {
            topicCallbacks.remove(topic)
        }

        return match (result) {
            case Ok(_) => Ok(())
            case Err(e) => Err(e)
        }
    }

    // 断开连接
    public func disconnect() {
        if (isConnected) {
            shouldReconnect = false
            reconnectAttempt = 0
            let disconnectPacket = codec.encodeDisconnect()
            try {
                transport.send(disconnectPacket)
            } catch (e: Exception) {
                // 忽略发送错误
            }

            stopKeepAlive()
            transport.close()
            isConnected = false
            onDisconnect()
        }
    }

    // 处理连接丢失（触发自动重连）
    private func handleConnectionLost(): Unit {
        isConnected = false
        if (options.autoReconnect) {
            startReconnect()
        } else {
            onDisconnect()
        }
    }

    // 自动重连
    private func startReconnect(): Unit {
        if (!options.autoReconnect || options.maxReconnectAttempts <= 0) {
            return
        }
        shouldReconnect = true
        spawn {
            while (shouldReconnect && reconnectAttempt < options.maxReconnectAttempts) {
                reconnectAttempt += 1
                var delayMs = options.reconnectBaseDelayMs * reconnectAttempt
                if (delayMs > 30000) { delayMs = 30000 }
                println("重连尝试 ${reconnectAttempt}/${options.maxReconnectAttempts} (等待 ${delayMs}ms)...")
                sleep(Duration.millisecond * delayMs)

                if (!shouldReconnect) { break }

                let r = connect()
                if (r.isOk()) {
                    println("重连成功 (session present=${this.sessionPresent})")
                    if (!this.sessionPresent) {
                        // 重新订阅（仅当 broker 未恢复会话时）
                        for ((topic, qos) in subscriptions) {
                            let packetId = messageStore.generatePacketId()
                            let subs = [Subscription(topic, qos: qos)]
                            let pkt = codec.encodeSubscribe(subs, packetId)
                            let _ = transport.send(pkt)
                        }
                    }
                    shouldReconnect = false
                    reconnectAttempt = 0
                    return
                } else {
                    println("重连失败: ${r.unwrapErr()}")
                }
            }
            if (reconnectAttempt >= options.maxReconnectAttempts) {
                onError("重连次数已达上限")
            }
            shouldReconnect = false
        }
    }

    // 处理接收到的数据
    private func handleReceivedData(data: Array<UInt8>) {
        let packetOpt = MqttDecoder.decode(data)

        if (packetOpt.isNone()) {
            onError("无法解析数据包")
            return
        }

        let packet = packetOpt.getOrThrow()

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
                lastPingRespTime = getCurrentTimeMillis()
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
    }

    // 处理 PUBACK (QoS 1 确认)
    private func handlePubAck(packet: MqttPacket): Unit {
        let result = codec.parsePubAck(packet)
        println("PUBACK (packetId=${result.packetId}, reasonCode=${result.reasonCode.toInt()})")

        // 从 inflight 消息中删除
        let removed = messageStore.remove(result.packetId)

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

        // 更新 inflight 状态为 WAIT_PUBREL
        let updated = messageStore.updateState(result.packetId, InflightState.WAIT_PUBREL)

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

        // 查找存储的 QoS 2 消息并交付
        let storedResult = qos2Incoming.remove(result.packetId)
        if (storedResult.isSome()) {
            let pubResult = storedResult.getOrThrow()

            // 交付消息给回调
            var callbackFound = false
            for ((topicFilter, callbackFn) in topicCallbacks) {
                if (TopicMatcher.matches(topic: pubResult.topic, topicFilter: topicFilter)) {
                    callbackFn(pubResult.topic, pubResult.payload)
                    callbackFound = true
                }
            }
            if (!callbackFound) {
                onMessage(pubResult.topic, pubResult.payload)
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

        // 从 inflight 消息中删除
        let removed = messageStore.remove(result.packetId)

        if (removed.isSome()) {
            println("消息已确认 (QoS 2 完成, packetId=${result.packetId})")
        } else {
            println("收到未知的 PUBCOMP (packetId=${result.packetId})")
        }
    }

    // 处理 CONNACK
    private func handleConnAck(packet: MqttPacket) {
        let result = codec.parseConnAck(packet)

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
                onError("连接被拒绝: ${errorCode}")
        }
    }

    // 处理 SUBACK
    private func handleSubAck(packet: MqttPacket): Unit {
        let result = codec.parseSubAck(packet)
        println("SUBACK 已确认 (packetId=${result.packetId})")
        for (qos in result.grantedQos) {
            match (qos) {
                case -1 => println("  主题订阅失败")
                case 0 => println("  主题订阅: QoS 0")
                case 1 => println("  主题订阅: QoS 1")
                case 2 => println("  主题订阅: QoS 2")
                case _ => println("  未知返回码: ${qos}")
            }
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

        // 使用通配符匹配 topic filter
        var callbackFound = false
        for ((topicFilter, callbackFn) in topicCallbacks) {
            if (TopicMatcher.matches(topic: result.topic, topicFilter: topicFilter)) {
                callbackFn(result.topic, result.payload)
                callbackFound = true
            }
        }

        // 如果没有找到 per-topic 回调，使用全局 onMessage
        if (!callbackFound) {
            onMessage(result.topic, result.payload)
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
                // 存储收到的 QoS 2 消息解析结果，供后续 PUBREL 交付回调使用
                qos2Incoming.add(result.packetId, result)
                // 发送 PUBREC
                let pubRecPacket = codec.encodePubRec(result.packetId)
                let pubRecResult = transport.send(pubRecPacket)
                if (pubRecResult.isErr()) {
                    onError("发送 PUBREC 失败: ${pubRecResult.unwrapErr()}")
                } else {
                    println("PUBREC 已发送 (packetId=${result.packetId})")
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
            lastPingReqTime = currentTime
            lastPingRespTime = currentTime

            // 启动后台线程
            spawn {
                this.keepAliveLoop()
            }
        }
    }

    // Keep Alive 后台循环
    private func keepAliveLoop(): Unit {
        while (isConnected) {
            try {
                let currentTime = getCurrentTimeMillis()
                let keepAliveMs = Int64(options.keepAlive) * 1000

                // 检查是否需要发送 PINGREQ (每 keepAlive/2 秒)
                if (currentTime - lastPingReqTime > keepAliveMs / 2) {
                    sendPingReq()
                }

                // 检查 PINGRESP 超时 (1.5 倍 keepAlive)
                if (currentTime - lastPingRespTime > keepAliveMs * 3 / 2) {
                    println("Keep Alive 超时，连接可能已断开")
                    onError("Keep Alive 超时")
                    handleConnectionLost()
                    break
                }

                // QoS 1/2 超时重传检测
                let timeoutMessages = messageStore.getTimeoutMessages()
                var maxRetries: Int64 = 3
                for (inflight in timeoutMessages) {
                    if (inflight.retryCount >= maxRetries) {
                        onError("消息重传超限 (packetId=${inflight.packetId})")
                        messageStore.remove(inflight.packetId)
                    } else {
                        // 设置 DUP 标志并重传
                        let rawPacket = inflight.packetData
                        if (rawPacket.size > 0) {
                            let dupHeader = rawPacket[0] | UInt8(0x08)
                            var retryData = Array<UInt8>(rawPacket.size, repeat: 0)
                            retryData[0] = dupHeader
                            for (i in 1..rawPacket.size) {
                                retryData[Int64(i)] = rawPacket[Int64(i)]
                            }
                            let retryResult = transport.send(retryData)
                            if (retryResult.isOk()) {
                                inflight.updateTimestamp()
                                println("消息重传 (packetId=${inflight.packetId}, retry=${inflight.retryCount})")
                            }
                        }
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
            lastPingReqTime = getCurrentTimeMillis()
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
        return isConnected && transport.isConnected()
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

// 字符串转 UTF-8 字节数组
private func stringToUtf8(s: String): Array<UInt8> {
    let result = ArrayList<UInt8>()
    for (b in s) {
        result.add(b)
    }
    let arr = Array<UInt8>(result.size, repeat: 0)
    for (i in 0..result.size) {
        arr[Int64(i)] = result[Int64(i)]
    }
    return arr
}
