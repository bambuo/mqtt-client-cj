// MQTT 协议编解码抽象基类
//
// 设计模式：
// - Strategy: ProtocolCodec → V311Codec / V50Codec（可互换的策略对象）
// - Template Method: 基类提供 encodeConnect / parseConnAck 等骨架，
//   子类通过 hooks 注入版本差异（Properties 写入/读取等）
// - Factory: ProtocolCodec.create(ProtocolVersion)
//
// 仓颉特性：abstract class / protected abstract func / static func / let

package mqtt.protocol

import std.convert.*
import std.collection.*

/// 协议编解码器抽象基类
public abstract class ProtocolCodec {

    // ============================================================
    // 编码模板方法
    // ============================================================

    /// 编码 CONNECT 包（模板方法）
    public func encodeConnect(options: ConnectOptions): Array<UInt8> {
        let buf = ByteArrayOutputStream()

        writeString(buf, "MQTT")
        buf.write(getProtocolLevel())

        let flags = ProtocolCodec.computeConnectFlags(options)
        buf.write(flags)
        buf.writeShort(UInt16(options.keepAlive))

        writeConnectProperties(buf, options)
        ProtocolCodec.writeConnectPayload(buf, options)

        let remaining = buf.size()
        let result = ByteArrayOutputStream()
        result.write(0x10)
        writeVariableByteInteger(result, remaining)
        result.writeAll(buf.toByteArray())
        return result.toByteArray()
    }

    /// 编码 PUBLISH 包（模板方法）
    public func encodePublish(topic: String, payload: Array<UInt8>, qos: QoS,
        packetId: Int, retain: Bool, dup: Bool,
        properties: MqttProperties): Array<UInt8> {
        let buf = ByteArrayOutputStream()

        writeString(buf, topic)
        if (qosToInt(qos) > 0) {
            buf.writeShort(UInt16(packetId))
        }
        writePublishProperties(buf, properties)
        buf.writeAll(payload)

        let remaining = buf.size()
        let result = ByteArrayOutputStream()
        var fixedHeader: UInt8 = 0x30
        fixedHeader = fixedHeader | (UInt8(qosToInt(qos)) << 1)
        if (retain) { fixedHeader = fixedHeader | 0x01 }
        if (dup) { fixedHeader = fixedHeader | 0x08 }
        result.write(fixedHeader)
        writeVariableByteInteger(result, remaining)
        result.writeAll(buf.toByteArray())
        return result.toByteArray()
    }

    /// 编码 SUBSCRIBE 包（模板方法）
    public func encodeSubscribe(subscriptions: Array<Subscription>, packetId: Int): Array<UInt8> {
        let buf = ByteArrayOutputStream()

        buf.writeShort(UInt16(packetId))
        writeSubscribeProperties(buf)
        for (sub in subscriptions) {
            writeString(buf, sub.topic)
            buf.write(UInt8(qosToInt(sub.qos)))
        }

        let remaining = buf.size()
        let result = ByteArrayOutputStream()
        result.write(0x82)
        writeVariableByteInteger(result, remaining)
        result.writeAll(buf.toByteArray())
        return result.toByteArray()
    }

    /// 编码 UNSUBSCRIBE 包（模板方法）
    public func encodeUnsubscribe(topics: Array<String>, packetId: Int): Array<UInt8> {
        let buf = ByteArrayOutputStream()

        buf.writeShort(UInt16(packetId))
        writeUnsubscribeProperties(buf)
        for (topic in topics) {
            writeString(buf, topic)
        }

        let remaining = buf.size()
        let result = ByteArrayOutputStream()
        result.write(0xA2)
        writeVariableByteInteger(result, remaining)
        result.writeAll(buf.toByteArray())
        return result.toByteArray()
    }

    // ---- 按版本独立实现的编码方法 ----

    public func encodeDisconnect(): Array<UInt8>
    public func encodeAuth(reasonCode: ReasonCode, properties: MqttProperties): Array<UInt8>

    // ---- 版本无关的编码方法 ----

    /// 编码 PINGREQ 包（所有版本一致）
    public func encodePingReq(): Array<UInt8> {
        return [0xC0, 0x00]
    }

    /// 编码 PUBACK 包（QoS 1 确认）
    public func encodePubAck(packetId: Int): Array<UInt8> {
        let buf = ByteArrayOutputStream()
        buf.writeShort(UInt16(packetId))
        let result = ByteArrayOutputStream()
        result.write(0x40)
        writeVariableByteInteger(result, 2)
        result.writeAll(buf.toByteArray())
        return result.toByteArray()
    }

    /// 编码 PUBREC 包
    public func encodePubRec(packetId: Int): Array<UInt8> {
        let buf = ByteArrayOutputStream()
        buf.writeShort(UInt16(packetId))
        let result = ByteArrayOutputStream()
        result.write(0x50)
        writeVariableByteInteger(result, 2)
        result.writeAll(buf.toByteArray())
        return result.toByteArray()
    }

    /// 编码 PUBREL 包
    public func encodePubRel(packetId: Int): Array<UInt8> {
        let buf = ByteArrayOutputStream()
        buf.writeShort(UInt16(packetId))
        let result = ByteArrayOutputStream()
        result.write(0x62)
        writeVariableByteInteger(result, 2)
        result.writeAll(buf.toByteArray())
        return result.toByteArray()
    }

    /// 编码 PUBCOMP 包
    public func encodePubComp(packetId: Int): Array<UInt8> {
        let buf = ByteArrayOutputStream()
        buf.writeShort(UInt16(packetId))
        let result = ByteArrayOutputStream()
        result.write(0x70)
        writeVariableByteInteger(result, 2)
        result.writeAll(buf.toByteArray())
        return result.toByteArray()
    }

    // ============================================================
    // 解码模板方法
    // ============================================================

    /// 解析 CONNACK 包（模板方法）
    public func parseConnAck(packet: MqttPacket): ConnAckResult {
        let input = ByteArrayInputStream(packet.payload)
        let flags = input.read()
        let sessionPresent = (flags & 0x01) != 0
        let returnCodeValue = input.read()
        let returnCode = ConnectReturnCode.fromInt(Int64(returnCodeValue))
        let properties = parseConnAckProperties(input, packet.payload.size)
        return ConnAckResult(sessionPresent, returnCode, properties)
    }

    /// 解析 PUBLISH 包（模板方法）
    public func parsePublish(packet: MqttPacket): PublishResult {
        let input = ByteArrayInputStream(packet.payload)
        let topic = readString(input)

        var packetId: Int = 0
        let qos = QoS.fromInt(Int64((packet.flags & 0x06) >> 1))
        match (qos) {
            case AT_MOST_ONCE => ()
            case _ => packetId = Int64(input.readShort())
        }

        let properties = parsePublishProperties(input, packet.payload.size)

        let payloadLength = packet.payload.size - Int64(input.getPosition())
        let payload = Array<UInt8>(payloadLength, repeat: 0)
        for (i in 0..payloadLength) {
            payload[Int64(i)] = input.read()
        }

        let retained = (packet.flags & UInt8(0x01)) != UInt8(0)
        return PublishResult(topic, payload, qos, retained, packetId, properties)
    }

    /// 解析 SUBACK 包（模板方法）
    public func parseSubAck(packet: MqttPacket): SubAckResult {
        let input = ByteArrayInputStream(packet.payload)
        let packetId = Int64(input.readShort())

        let properties = parseSubAckProperties(input, packet.payload.size)

        let remaining = packet.payload.size - Int64(input.getPosition())
        var qosList = ArrayList<Int64>()
        for (i in 0..remaining) {
            let code = Int64(input.read())
            let granted = if (code < 0x80) { code } else { -1 }
            qosList.add(granted)
        }
        let grantedQos = Array<Int64>(qosList.size, repeat: 0)
        for (i in 0..qosList.size) {
            grantedQos[Int64(i)] = qosList[Int64(i)]
        }
        return SubAckResult(packetId, grantedQos, properties)
    }

    // ---- 按版本独立实现的解码方法 ----

    public func parsePubAck(packet: MqttPacket): PubAckResult
    public func parsePubRec(packet: MqttPacket): PubRecResult
    public func parsePubRel(packet: MqttPacket): PubRelResult
    public func parsePubComp(packet: MqttPacket): PubCompResult
    public func parseUnsubAck(packet: MqttPacket): UnsubAckResult
    public func parseDisconnect(packet: MqttPacket): DisconnectResult
    public func parseAuth(packet: MqttPacket): AuthResult

    // ============================================================
    // Abstract Hooks（子类注入版本差异）
    // ============================================================

    /// CONNECT 协议级别：V311→4, V50→5
    protected func getProtocolLevel(): UInt8

    /// 写入 CONNECT Properties（V311→空, V50→options.properties）
    protected func writeConnectProperties(buf: ByteArrayOutputStream, options: ConnectOptions): Unit

    /// 写入 PUBLISH Properties（V311→空, V50→properties.encode()）
    protected func writePublishProperties(buf: ByteArrayOutputStream, properties: MqttProperties): Unit

    /// 写入 SUBSCRIBE Properties 长度（V311→空, V50→写 0）
    protected func writeSubscribeProperties(buf: ByteArrayOutputStream): Unit

    /// 写入 UNSUBSCRIBE Properties 长度（V311→空, V50→写 0）
    protected func writeUnsubscribeProperties(buf: ByteArrayOutputStream): Unit

    /// 解析 CONNACK Properties（V311→返回空, V50→读字节流）
    protected func parseConnAckProperties(input: ByteArrayInputStream, totalSize: Int64): MqttProperties

    /// 解析 PUBLISH Properties（V311→返回空, V50→读 VBI+字节流）
    protected func parsePublishProperties(input: ByteArrayInputStream, totalSize: Int64): MqttProperties

    /// 解析 SUBACK Properties（V311→返回空, V50→读 VBI+字节流）
    protected func parseSubAckProperties(input: ByteArrayInputStream, totalSize: Int64): MqttProperties

    // ============================================================
    // 共享解码辅助
    // ============================================================

    /// 解析 QoS 响应包（PUBACK/PUBREC/PUBREL/PUBCOMP）的公共逻辑
    /// @return (packetId, reasonCode, properties)
    protected func parseQosResponse(packet: MqttPacket): (Int, ReasonCode, MqttProperties) {
        let input = ByteArrayInputStream(packet.payload)
        let packetId = Int64(input.readShort())

        if (input.getPosition() >= packet.payload.size) {
            return (packetId, ReasonCode.Success, MqttProperties())
        }

        let reasonCodeValue = Int64(input.read())
        let reasonCode = ReasonCode.fromInt(reasonCodeValue)
        var properties = MqttProperties()

        if (input.getPosition() < packet.payload.size) {
            let propertiesLength = readVariableByteInteger(input)
            if (propertiesLength > 0) {
                let propBytes = readRemainingBytes(input, propertiesLength)
                properties = MqttProperties.decode(propBytes)
            }
        }

        return (packetId, reasonCode, properties)
    }

    // ============================================================
    // 共享编码辅助（static，两版本复用）
    // ============================================================

    /// 计算 CONNECT 包的 flags 字节
    protected static func computeConnectFlags(options: ConnectOptions): UInt8 {
        var flags: UInt8 = 0
        if (options.cleanStart) {
            flags = flags | 0b00000010
        }
        if (options.willTopic.isSome()) {
            flags = flags | 0b00000100
            flags = flags | (UInt8(qosToInt(options.willQos)) << 3)
            if (options.willRetain) {
                flags = flags | 0b00100000
            }
        }
        if (options.password.isSome()) {
            flags = flags | 0b01000000
        }
        if (options.username.isSome()) {
            flags = flags | 0b10000000
        }
        return flags
    }

    /// 写入 CONNECT 包的 Payload（clientId + will + username + password）
    protected static func writeConnectPayload(buf: ByteArrayOutputStream, options: ConnectOptions): Unit {
        writeString(buf, options.clientId)
        if (options.willTopic.isSome()) {
            writeString(buf, options.willTopic.getOrThrow())
            let willPayload = options.willPayload ?? Array<UInt8>()
            writeBinary(buf, willPayload)
        }
        if (options.username.isSome()) {
            writeString(buf, options.username.getOrThrow())
        }
        if (options.password.isSome()) {
            writeBinary(buf, options.password.getOrThrow())
        }
    }

    // ============================================================
    // 公共辅助方法
    // ============================================================

    protected static func writeString(buf: ByteArrayOutputStream, str: String): Unit {
        let bytes = stringToUtf8Arr(str)
        buf.writeShort(UInt16(bytes.size))
        buf.writeAll(bytes)
    }

    protected static func writeBinary(buf: ByteArrayOutputStream, data: Array<UInt8>): Unit {
        buf.writeShort(UInt16(data.size))
        buf.writeAll(data)
    }

    protected static func writeVariableByteInteger(buf: ByteArrayOutputStream, value: Int64): Unit {
        var val = value
        do {
            var byte = UInt8(val % 128)
            val = val / 128
            if (val > 0) { byte = byte | 0x80 }
            buf.write(byte)
        } while (val > 0)
    }

    protected static func readString(input: ByteArrayInputStream): String {
        let len = Int64(input.readShort())
        let bytes = Array<UInt8>(len, repeat: 0)
        for (i in 0..len) {
            bytes[Int64(i)] = input.read()
        }
        return String.fromUtf8(bytes)
    }

    protected static func readBinary(input: ByteArrayInputStream): Array<UInt8> {
        let len = Int64(input.readShort())
        let bytes = Array<UInt8>(len, repeat: 0)
        for (i in 0..len) {
            bytes[Int64(i)] = input.read()
        }
        return bytes
    }

    protected static func readVariableByteInteger(input: ByteArrayInputStream): Int {
        var result: Int = 0
        var multiplier: Int = 1
        var byte: Int64 = 0
        do {
            byte = Int64(input.read())
            result = result + (byte & 0x7F) * multiplier
            multiplier = multiplier * 128
        } while ((byte & 0x80) != 0)
        return result
    }

    protected static func readRemainingBytes(input: ByteArrayInputStream, length: Int): Array<UInt8> {
        let bytes = Array<UInt8>(Int64(length), repeat: 0)
        for (i in 0..Int64(length)) {
            bytes[Int64(i)] = input.read()
        }
        return bytes
    }

    protected static func qosToInt(qos: QoS): Int {
        match (qos) {
            case AT_MOST_ONCE => 0
            case AT_LEAST_ONCE => 1
            case EXACTLY_ONCE => 2
        }
    }

    // ============================================================
    // Factory
    // ============================================================

    public static func create(version: ProtocolVersion): ProtocolCodec {
        match (version) {
            case V3_1_1 => V311Codec()
            case V5_0 => V50Codec()
        }
    }

    // ---- 私有辅助 ----

    private static func stringToUtf8Arr(str: String): Array<UInt8> {
        var result = ArrayList<UInt8>()
        for (c in str) {
            let code = UInt32(c)
            if (code < UInt32(0x80)) {
                result.add(UInt8(code))
            } else if (code < UInt32(0x800)) {
                result.add(UInt8(0xC0 | (code >> 6)))
                result.add(UInt8(0x80 | (code & 0x3F)))
            } else {
                result.add(UInt8(0xE0 | (code >> 12)))
                result.add(UInt8(0x80 | ((code >> 6) & 0x3F)))
                result.add(UInt8(0x80 | (code & 0x3F)))
            }
        }
        let sz = result.size
        let arr = Array<UInt8>(sz, repeat: 0)
        for (i in Int64(0)..sz) {
            arr[Int64(i)] = result[Int64(i)]
        }
        return arr
    }
}
