// MQTT v5.0 协议编解码实现
//
// 继承 ProtocolCodec 抽象类。
// 编码/解码骨架由基类 Template Method 提供，本类注入 v5.0 特有的 hooks：
// - getProtocolLevel → 5
// - writeXxxProperties / parseXxxProperties → Properties 编解码
// QoS 响应解析（parsePubAck/Rec/Rel/Comp）复用基类 parseQosResponse 消除重复。

package mqtt.protocol

public class V50Codec <: ProtocolCodec {

    // ==================== Hooks（基类模板方法调用） ====================

    public override func getProtocolLevel(): UInt8 {
        return 5
    }

    public override func writeConnectProperties(buf: ByteArrayOutputStream, options: ConnectOptions): Unit {
        let propsData = options.properties.encode()
        if (propsData.size > 0) {
            writeVariableByteInteger(buf, propsData.size)
            buf.writeAll(propsData)
        } else {
            buf.write(0)
        }
    }

    public override func writePublishProperties(buf: ByteArrayOutputStream, properties: MqttProperties): Unit {
        let propsData = properties.encode()
        writeVariableByteInteger(buf, propsData.size)
        buf.writeAll(propsData)
    }

    public override func writeSubscribeProperties(buf: ByteArrayOutputStream, properties: MqttProperties): Unit {
        let propsData = properties.encode()
        if (propsData.size > 0) {
            writeVariableByteInteger(buf, propsData.size)
            buf.writeAll(propsData)
        } else {
            buf.write(0)  // Properties Length = 0
        }
    }

    // v5.0：Subscription Options 字节，QoS 在 bits 2-1，bit0 reserved=0（MQTT-3.8.3.1）
    public override func encodeSubscriptionOptions(sub: Subscription): UInt8 {
        return UInt8(qosToInt(sub.qos) << 1)
    }

    public override func writeUnsubscribeProperties(buf: ByteArrayOutputStream): Unit {
        buf.write(0)  // Properties Length = 0
    }

    public override func parseConnAckProperties(input: ByteArrayInputStream, totalSize: Int64): MqttProperties {
        var properties = MqttProperties()
        if (input.getPosition() < totalSize) {
            let propertiesLength = readVariableByteInteger(input)
            if (propertiesLength > 0) {
                let propBytes = readRemainingBytes(input, propertiesLength)
                properties = MqttProperties.decode(propBytes)
            }
        }
        return properties
    }

    /// v5.0 CONNACK：正确解析 Reason Code（可能为 128-159），
    /// 同时保留 v3.1.1 兼容的 returnCode 字段。
    public override func parseConnAck(packet: MqttPacket): ConnAckResult {
        let input = ByteArrayInputStream(packet.payload)
        let flags = input.read()
        // [MQTT-3.2.2-3] Acknowledge Flags bits 7-1 保留，必须为 0（v5.0 畸形包处理）
        if ((flags & 0xFE) != 0) {
            throw ProtocolError("CONNACK Acknowledge Flags reserved bits 7-1 must be 0")
        }
        let sessionPresent = (flags & 0x01) != 0
        let reasonCodeValue = Int64(input.read())
        let reasonCode = ReasonCode.fromInt(reasonCodeValue)
        // v5.0 中 returnCode 字段仍尽量映射（0=ACCEPTED, 非0=映射最近似值）
        let returnCode = if (reasonCodeValue == 0) {
            ConnectReturnCode.ACCEPTED
        } else {
            ConnectReturnCode.SERVER_UNAVAILABLE
        }
        let properties = parseConnAckProperties(input, packet.payload.size)
        return ConnAckResult(sessionPresent, returnCode, properties, reasonCode: reasonCode)
    }

    public override func parsePublishProperties(input: ByteArrayInputStream, totalSize: Int64): MqttProperties {
        var properties = MqttProperties()
        let propertiesLength = readVariableByteInteger(input)
        if (propertiesLength > 0) {
            let propBytes = readRemainingBytes(input, propertiesLength)
            properties = MqttProperties.decode(propBytes)
        }
        return properties
    }

    public override func parseSubAckProperties(input: ByteArrayInputStream, totalSize: Int64): MqttProperties {
        var properties = MqttProperties()
        if (input.getPosition() < totalSize) {
            let propertiesLength = readVariableByteInteger(input)
            if (propertiesLength > 0) {
                let propBytes = readRemainingBytes(input, propertiesLength)
                properties = MqttProperties.decode(propBytes)
            }
        }
        return properties
    }

    // ==================== 编码（独立实现） ====================

    public override func encodeDisconnect(reasonCode: ReasonCode, properties: MqttProperties): Array<UInt8> {
        let buf = ByteArrayOutputStream()
        buf.write(UInt8(reasonCode.toInt()))
        let propsData = properties.encode()
        writeVariableByteInteger(buf, propsData.size)
        buf.writeAll(propsData)

        let remaining = buf.size()
        let result = ByteArrayOutputStream()
        result.write(0xE0)
        writeVariableByteInteger(result, remaining)
        result.writeAll(buf.toByteArray())
        return result.toByteArray()
    }

    public override func encodeAuth(reasonCode: ReasonCode, properties: MqttProperties): Array<UInt8> {
        let buf = ByteArrayOutputStream()
        buf.write(UInt8(reasonCode.toInt()))
        let propsData = properties.encode()
        writeVariableByteInteger(buf, propsData.size)
        buf.writeAll(propsData)

        let remaining = buf.size()
        let result = ByteArrayOutputStream()
        result.write(0xF0)
        writeVariableByteInteger(result, remaining)
        result.writeAll(buf.toByteArray())
        return result.toByteArray()
    }

    // ==================== 解码（独立实现，复用 parseQosResponse） ====================

    public override func parsePubAck(packet: MqttPacket): PubAckResult {
        let (packetId, reasonCode, properties) = parseQosResponse(packet)
        let success = (reasonCode.toInt() < 0x80)
        return PubAckResult(packetId, success, reasonCode, properties)
    }

    public override func parsePubRec(packet: MqttPacket): PubRecResult {
        let (packetId, reasonCode, properties) = parseQosResponse(packet)
        let success = (reasonCode.toInt() < 0x80)
        return PubRecResult(packetId, success, reasonCode, properties)
    }

    public override func parsePubRel(packet: MqttPacket): PubRelResult {
        let (packetId, reasonCode, properties) = parseQosResponse(packet)
        let success = (reasonCode.toInt() < 0x80)
        return PubRelResult(packetId, success, reasonCode, properties)
    }

    public override func parsePubComp(packet: MqttPacket): PubCompResult {
        let (packetId, reasonCode, properties) = parseQosResponse(packet)
        let success = (reasonCode.toInt() < 0x80)
        return PubCompResult(packetId, success, reasonCode, properties)
    }

    public override func parseUnsubAck(packet: MqttPacket): UnsubAckResult {
        let input = ByteArrayInputStream(packet.payload)
        let packetId = Int64(input.readShort())
        var success = true
        var properties = MqttProperties()

        // v5.0 UNSUBACK 总是包含 Properties Length 字段
        if (input.getPosition() < packet.payload.size) {
            let propertiesLength = readVariableByteInteger(input)
            if (propertiesLength > 0) {
                let propBytes = readRemainingBytes(input, propertiesLength)
                properties = MqttProperties.decode(propBytes)
            }
            // 剩余字节为 Reason Codes（每主题一个）
            let remaining = packet.payload.size - Int64(input.getPosition())
            for (i in 0..remaining) {
                let code = input.read()
                if (code >= UInt8(0x80)) { success = false }
            }
        }
        // 注意：严格按照 v5.0 协议，UNSUBACK 至少应包含 Properties Length 字节。
        // 若 payload 仅含 packetId（2 字节），说明是对端实现不完整，此处容错处理。
        return UnsubAckResult(packetId, success, properties)
    }

    public override func parseDisconnect(packet: MqttPacket): DisconnectResult {
        let input = ByteArrayInputStream(packet.payload)
        if (input.getPosition() >= packet.payload.size) {
            return DisconnectResult(ReasonCode.Success, MqttProperties())
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
        return DisconnectResult(reasonCode, properties)
    }

    public override func parseAuth(packet: MqttPacket): AuthResult {
        let input = ByteArrayInputStream(packet.payload)
        if (input.getPosition() >= packet.payload.size) {
            return AuthResult(ReasonCode.Success, MqttProperties())
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
        return AuthResult(reasonCode, properties)
    }
}
