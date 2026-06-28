// MQTT v5.0 协议编解码实现
//
// 继承 ProtocolCodec 抽象类。
// 编码/解码骨架由基类 Template Method 提供，本类注入 v5.0 特有的 hooks：
// - getProtocolLevel → 5
// - writeXxxProperties / parseXxxProperties → Properties 编解码
// QoS 响应解析（parsePubAck/Rec/Rel/Comp）复用基类 parseQosResponse 消除重复。

package mqtt.protocol

import std.collection.*

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

    public override func writeSubscribeProperties(buf: ByteArrayOutputStream): Unit {
        buf.write(0)  // Properties Length = 0
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

    public override func encodeDisconnect(): Array<UInt8> {
        let result = ByteArrayOutputStream()
        result.write(0xE0)
        result.write(0x02)
        result.write(0x00)  // Reason code (success)
        result.write(0x00)  // Properties length
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
        result.write(0x1F)
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
        if (input.getPosition() < packet.payload.size) {
            let propertiesLength = readVariableByteInteger(input)
            if (propertiesLength > 0) {
                let propBytes = readRemainingBytes(input, propertiesLength)
                properties = MqttProperties.decode(propBytes)
            }
            let remaining = packet.payload.size - Int64(input.getPosition())
            for (i in 0..remaining) {
                let code = input.read()
                if (code >= UInt8(0x80)) { success = false }
            }
        }
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
