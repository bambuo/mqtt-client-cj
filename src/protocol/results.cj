// MQTT 协议统一结果类型
//
// 封装 v3.1.1 和 v5.0 的解析结果，消除客户端版本分支。
// v3.1.1 场景下 v5.0 特有字段填入默认值（空 MqttProperties / Success）。

package mqtt.protocol

/// CONNACK 解析结果
public class ConnAckResult {
    public var sessionPresent: Bool
    public var returnCode: ConnectReturnCode
    public var reasonCode: ReasonCode
    public var properties: MqttProperties

    public init(sessionPresent: Bool, returnCode: ConnectReturnCode, properties: MqttProperties,
        reasonCode!: ReasonCode = ReasonCode.Success) {
        this.sessionPresent = sessionPresent
        this.returnCode = returnCode
        this.reasonCode = reasonCode
        this.properties = properties
    }
}

/// PUBLISH 解析结果
public class PublishResult {
    public var topic: String
    public var payload: Array<UInt8>
    public var qos: QoS
    public var retained: Bool
    public var dup: Bool
    public var packetId: Int
    public var properties: MqttProperties

    public init(topic: String, payload: Array<UInt8>, qos: QoS, retained: Bool,
        dup: Bool, packetId: Int, properties: MqttProperties) {
        this.topic = topic
        this.payload = payload
        this.qos = qos
        this.retained = retained
        this.dup = dup
        this.packetId = packetId
        this.properties = properties
    }
}

/// PUBACK 解析结果 (QoS 1 确认)
public class PubAckResult {
    public var packetId: Int
    public var success: Bool
    public var reasonCode: ReasonCode
    public var properties: MqttProperties

    public init(packetId: Int, success: Bool, reasonCode: ReasonCode, properties: MqttProperties) {
        this.packetId = packetId
        this.success = success
        this.reasonCode = reasonCode
        this.properties = properties
    }
}

/// PUBREC 解析结果 (QoS 2 第一步响应)
public class PubRecResult {
    public var packetId: Int
    public var success: Bool
    public var reasonCode: ReasonCode
    public var properties: MqttProperties

    public init(packetId: Int, success: Bool, reasonCode: ReasonCode, properties: MqttProperties) {
        this.packetId = packetId
        this.success = success
        this.reasonCode = reasonCode
        this.properties = properties
    }
}

/// PUBREL 解析结果 (QoS 2 第三步)
public class PubRelResult {
    public var packetId: Int
    public var success: Bool
    public var reasonCode: ReasonCode
    public var properties: MqttProperties

    public init(packetId: Int, success: Bool, reasonCode: ReasonCode, properties: MqttProperties) {
        this.packetId = packetId
        this.success = success
        this.reasonCode = reasonCode
        this.properties = properties
    }
}

/// PUBCOMP 解析结果 (QoS 2 最后一步)
public class PubCompResult {
    public var packetId: Int
    public var success: Bool
    public var reasonCode: ReasonCode
    public var properties: MqttProperties

    public init(packetId: Int, success: Bool, reasonCode: ReasonCode, properties: MqttProperties) {
        this.packetId = packetId
        this.success = success
        this.reasonCode = reasonCode
        this.properties = properties
    }
}

/// SUBACK 解析结果
/// grantedQos: -1=失败, 0/1/2=授予的 QoS 等级（两版本归一）
public class SubAckResult {
    public var packetId: Int64
    public var grantedQos: Array<Int64>
    public var properties: MqttProperties

    public init(packetId: Int64, grantedQos: Array<Int64>, properties: MqttProperties) {
        this.packetId = packetId
        this.grantedQos = grantedQos
        this.properties = properties
    }
}

/// UNSUBACK 解析结果
public class UnsubAckResult {
    public var packetId: Int64
    public var success: Bool
    public var properties: MqttProperties

    public init(packetId: Int64, success: Bool, properties: MqttProperties) {
        this.packetId = packetId
        this.success = success
        this.properties = properties
    }
}

/// DISCONNECT 解析结果
public class DisconnectResult {
    public var reasonCode: ReasonCode
    public var properties: MqttProperties

    public init(reasonCode: ReasonCode, properties: MqttProperties) {
        this.reasonCode = reasonCode
        this.properties = properties
    }
}

/// AUTH 解析结果
public class AuthResult {
    public var reasonCode: ReasonCode
    public var properties: MqttProperties

    public init(reasonCode: ReasonCode, properties: MqttProperties) {
        this.reasonCode = reasonCode
        this.properties = properties
    }
}
