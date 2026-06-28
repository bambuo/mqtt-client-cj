// MQTT v3.1.1 协议编解码实现
//
// 继承 ProtocolCodec 抽象类。
// 编码/解码骨架由基类 Template Method 提供，本类仅注入 v3.1.1 特有的 hooks：
// - getProtocolLevel → 4
// - writeXxxProperties / parseXxxProperties → 空操作
// 独立方法（encodeDisconnect / encodeAuth 等）因两版本差异大，仍在本地实现。

package mqtt.protocol

public class V311Codec <: ProtocolCodec {

    // ==================== Hooks（基类模板方法调用） ====================

    public override func getProtocolLevel(): UInt8 {
        return 4
    }

    public override func writeConnectProperties(buf: ByteArrayOutputStream, options: ConnectOptions): Unit {
    }

    public override func writePublishProperties(buf: ByteArrayOutputStream, properties: MqttProperties): Unit {
    }

    public override func writeSubscribeProperties(buf: ByteArrayOutputStream, properties: MqttProperties): Unit {
    }

    // v3.1.1：Subscription Options 字节即 QoS 原值（0/1/2）
    public override func encodeSubscriptionOptions(sub: Subscription): UInt8 {
        return UInt8(qosToInt(sub.qos))
    }

    public override func writeUnsubscribeProperties(buf: ByteArrayOutputStream): Unit {
    }

    public override func parseConnAckProperties(input: ByteArrayInputStream, totalSize: Int64): MqttProperties {
        return MqttProperties()
    }

    public override func parsePublishProperties(input: ByteArrayInputStream, totalSize: Int64): MqttProperties {
        return MqttProperties()
    }

    public override func parseSubAckProperties(input: ByteArrayInputStream, totalSize: Int64): MqttProperties {
        return MqttProperties()
    }

    // ==================== 编码（独立实现） ====================

    public override func encodeDisconnect(reasonCode: ReasonCode, properties: MqttProperties): Array<UInt8> {
        return [0xE0, 0x00]
    }

    public override func encodeAuth(reasonCode: ReasonCode, properties: MqttProperties): Array<UInt8> {
        throw Exception("AUTH 包在 MQTT v3.1.1 中不存在")
    }

    // ==================== 解码（独立实现） ====================

    public override func parsePubAck(packet: MqttPacket): PubAckResult {
        let input = ByteArrayInputStream(packet.payload)
        let packetId = Int64(input.readShort())
        return PubAckResult(packetId, true, ReasonCode.Success, MqttProperties())
    }

    public override func parsePubRec(packet: MqttPacket): PubRecResult {
        let input = ByteArrayInputStream(packet.payload)
        let packetId = Int64(input.readShort())
        return PubRecResult(packetId, true, ReasonCode.Success, MqttProperties())
    }

    public override func parsePubRel(packet: MqttPacket): PubRelResult {
        let input = ByteArrayInputStream(packet.payload)
        let packetId = Int64(input.readShort())
        return PubRelResult(packetId, true, ReasonCode.Success, MqttProperties())
    }

    public override func parsePubComp(packet: MqttPacket): PubCompResult {
        let input = ByteArrayInputStream(packet.payload)
        let packetId = Int64(input.readShort())
        return PubCompResult(packetId, true, ReasonCode.Success, MqttProperties())
    }

    public override func parseUnsubAck(packet: MqttPacket): UnsubAckResult {
        let input = ByteArrayInputStream(packet.payload)
        let packetId = Int64(input.readShort())
        return UnsubAckResult(packetId, true, MqttProperties())
    }

    public override func parseDisconnect(packet: MqttPacket): DisconnectResult {
        return DisconnectResult(ReasonCode.Success, MqttProperties())
    }

    public override func parseAuth(packet: MqttPacket): AuthResult {
        return AuthResult(ReasonCode.Success, MqttProperties())
    }
}
