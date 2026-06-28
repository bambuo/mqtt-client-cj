// MQTT 协议类型定义
package mqtt.protocol

import std.collection.*

// 通用 Result 类型（Cangjie 标准库不内置 Result，此处自行定义）
public enum Result<T, E> {
    | Ok(T)
    | Err(E)

    public func isOk(): Bool {
        match (this) {
            case Ok(_) => true
            case Err(_) => false
        }
    }

    public func isErr(): Bool {
        !isOk()
    }

    public func unwrap(): T {
        match (this) {
            case Ok(v) => v
            case Err(_) => throw NoneValueException("unwrap on Err value")
        }
    }

    public func unwrapErr(): E {
        match (this) {
            case Err(e) => e
            case Ok(_) => throw NoneValueException("unwrapErr on Ok value")
        }
    }
}

// MQTT 网络连接错误
public class ConnectionError <: Exception {
    public init(message: String) {
        super(message)
    }
}

// MQTT 协议错误
public class ProtocolError <: Exception {
    public init(message: String) {
        super(message)
    }
}

// MQTT 协议版本
public enum ProtocolVersion {
    | V3_1_1  // MQTT v3.1.1 (最广泛使用的版本)
    | V5_0    // MQTT v5.0 (最新版本)

    public func toInt(): Int {
        match (this) {
            case V3_1_1 => 4
            case V5_0 => 5
        }
    }

    public static func fromInt(value: Int): ProtocolVersion {
        match (value) {
            case 4 => V3_1_1
            case 5 => V5_0
            case _ => V3_1_1
        }
    }
}

// MQTT 控制包类型
public enum PacketType {
    | CONNECT
    | CONNACK
    | PUBLISH
    | PUBACK
    | PUBREC
    | PUBREL
    | PUBCOMP
    | SUBSCRIBE
    | SUBACK
    | UNSUBSCRIBE
    | UNSUBACK
    | PINGREQ
    | PINGRESP
    | DISCONNECT
    | AUTH  // v5.0 only

    public func toInt(): Int {
        match (this) {
            case CONNECT => 1
            case CONNACK => 2
            case PUBLISH => 3
            case PUBACK => 4
            case PUBREC => 5
            case PUBREL => 6
            case PUBCOMP => 7
            case SUBSCRIBE => 8
            case SUBACK => 9
            case UNSUBSCRIBE => 10
            case UNSUBACK => 11
            case PINGREQ => 12
            case PINGRESP => 13
            case DISCONNECT => 14
            case AUTH => 15
        }
    }

    public static func fromInt(value: Int): PacketType {
        match (value) {
            case 1 => CONNECT
            case 2 => CONNACK
            case 3 => PUBLISH
            case 4 => PUBACK
            case 5 => PUBREC
            case 6 => PUBREL
            case 7 => PUBCOMP
            case 8 => SUBSCRIBE
            case 9 => SUBACK
            case 10 => UNSUBSCRIBE
            case 11 => UNSUBACK
            case 12 => PINGREQ
            case 13 => PINGRESP
            case 14 => DISCONNECT
            case 15 => AUTH
            case _ => CONNECT
        }
    }
}

// QoS 等级
public enum QoS {
    | AT_MOST_ONCE
    | AT_LEAST_ONCE
    | EXACTLY_ONCE

    public func toInt(): Int64 {
        match (this) {
            case AT_MOST_ONCE => 0
            case AT_LEAST_ONCE => 1
            case EXACTLY_ONCE => 2
        }
    }

    public static func fromInt(value: Int64): QoS {
        match (value) {
            case 0 => AT_MOST_ONCE
            case 1 => AT_LEAST_ONCE
            case 2 => EXACTLY_ONCE
            case _ => AT_MOST_ONCE
        }
    }
}

// 连接返回码 (CONNACK)
public enum ConnectReturnCode {
    | ACCEPTED
    | WRONG_VERSION
    | INVALID_ID
    | SERVER_UNAVAILABLE
    | BAD_CREDENTIALS
    | NOT_AUTHORIZED

    public func toInt(): Int64 {
        match (this) {
            case ACCEPTED => 0
            case WRONG_VERSION => 1
            case INVALID_ID => 2
            case SERVER_UNAVAILABLE => 3
            case BAD_CREDENTIALS => 4
            case NOT_AUTHORIZED => 5
        }
    }

    public static func fromInt(value: Int): ConnectReturnCode {
        match (value) {
            case 0 => ACCEPTED
            case 1 => WRONG_VERSION
            case 2 => INVALID_ID
            case 3 => SERVER_UNAVAILABLE
            case 4 => BAD_CREDENTIALS
            case 5 => NOT_AUTHORIZED
            case _ => SERVER_UNAVAILABLE  // 非法值映射为 SERVER_UNAVAILABLE（而非 ACCEPTED），调用方应校验
        }
    }
}

// TLS 安全连接配置
public class TlsOptions {
    // 证书验证模式: 0=系统CA, 1=信任全部(测试用), 2=自定义CA
    public var verifyMode: Int = 1
    public var caCertPath: ?String = None  // 自定义 CA 证书路径
    public var clientCertPath: ?String = None  // 客户端证书路径（双向认证）
    public var clientKeyPath: ?String = None  // 客户端私钥路径
    public var serverName: ?String = None  // SNI 服务器名
    
    public init() {}
}

// v5.0 Properties 标识常量
public class PropId {
    public static let sessionExpiryInterval: UInt8 = 0x11
    public static let receiveMaximum: UInt8 = 0x21
    public static let maximumPacketSize: UInt8 = 0x27
    public static let topicAliasMaximum: UInt8 = 0x22
    public static let topicAlias: UInt8 = 0x23
    public static let requestResponseInformation: UInt8 = 0x19
    public static let requestProblemInformation: UInt8 = 0x17  // MQTT-5.0 §3.1.2.11.5
    public static let userProperty: UInt8 = 0x26
    public static let payloadFormatIndicator: UInt8 = 0x01
    public static let messageExpiryInterval: UInt8 = 0x02
    public static let responseTopic: UInt8 = 0x08
    public static let correlationData: UInt8 = 0x09
    public static let subscriptionIdentifier: UInt8 = 0x0B
    public static let contentTyp: UInt8 = 0x03             // Content Type (UTF-8)
    public static let willDelayInterval: UInt8 = 0x18      // Will Delay Interval (4B int)
    public static let sessionExpiryIntervalConnAck: UInt8 = 0x11
    public static let serverKeepAlive: UInt8 = 0x13
    public static let reasonString: UInt8 = 0x1F
    public static let authenticationMethod: UInt8 = 0x15
    public static let authenticationData: UInt8 = 0x16
    public static let maximumQoS: UInt8 = 0x24
    public static let retainAvailable: UInt8 = 0x25
    public static let wildcardSubscriptionAvailable: UInt8 = 0x28
    public static let subscriptionIdentifierAvailable: UInt8 = 0x29
    public static let sharedSubscriptionAvailable: UInt8 = 0x2A
    public static let assignedClientIdentifier: UInt8 = 0x12
    public static let serverReference: UInt8 = 0x1C
    public static let responseInformation: UInt8 = 0x1B
}

// v5.0 Properties 集合
public class MqttProperties {
    // CONNECT 属性
    public var sessionExpiryInterval: ?Int64 = None
    public var receiveMaximum: ?Int64 = None
    public var maximumPacketSize: ?Int64 = None
    public var topicAliasMaximum: ?Int64 = None
    public var requestResponseInformation: ?Bool = None
    public var requestProblemInformation: ?Bool = None
    // PUBLISH 属性
    public var payloadFormatIndicator: ?Bool = None
    public var messageExpiryInterval: ?Int64 = None
    public var responseTopic: ?String = None
    public var correlationData: ?Array<UInt8> = None
    public var subscriptionIdentifier: ?Int64 = None
    public var topicAlias: ?Int64 = None
    public var contentType: ?String = None                    // 0x03 Content Type
    public var willDelayInterval: ?Int64 = None              // 0x18 Will Delay Interval
    // 通用属性
    public var userProperties: HashMap<String, String> = HashMap()
    // CONNACK 属性
    public var serverKeepAlive: ?Int64 = None
    public var assignedClientIdentifier: ?String = None
    public var serverReference: ?String = None
    public var responseInformation: ?String = None
    // 通用响应属性
    public var reasonString: ?String = None
    // 认证属性
    public var authenticationMethod: ?String = None
    public var authenticationData: ?Array<UInt8> = None
    // Broker 能力属性
    public var maximumQoS: ?Int64 = None
    public var retainAvailable: ?Bool = None
    public var wildcardSubscriptionAvailable: ?Bool = None
    public var subscriptionIdentifierAvailable: ?Bool = None
    public var sharedSubscriptionAvailable: ?Bool = None

    // 编码 Properties 为字节数组
    public func encode(): Array<UInt8> {
        let buf = ArrayList<UInt8>()

        // 辅助写入函数
        func writeUint16(v: Int64): Unit {
            buf.add(UInt8((v >> 8) & 0xFF))
            buf.add(UInt8(v & 0xFF))
        }
        func writeUint32(v: Int64): Unit {
            buf.add(UInt8((v >> 24) & 0xFF))
            buf.add(UInt8((v >> 16) & 0xFF))
            buf.add(UInt8((v >> 8) & 0xFF))
            buf.add(UInt8(v & 0xFF))
        }
        func writeVbi(v: Int64): Unit {
            if (v < 0 || v > 268435455) {
                throw ProtocolError("VBI 值超出合法范围: ${v}")
            }
            var val = v
            do {
                var byte = UInt8(val % 128)
                val = val / 128
                if (val > 0) { byte = byte | 0x80 }
                buf.add(byte)
            } while (val > 0)
        }
        func writeUtf8(s: String): Unit {
            let bytes = encodeUtf8(s)
            let size = Int64(bytes.size)
            writeUint16(size)
            for (b in bytes) { buf.add(b) }
        }
        func writeBinary(data: Array<UInt8>): Unit {
            writeUint16(Int64(data.size))
            for (b in data) { buf.add(b) }
        }

        // 四字节整数属性
        if (sessionExpiryInterval.isSome()) {
            let v = sessionExpiryInterval.getOrThrow()
            buf.add(PropId.sessionExpiryInterval); writeUint32(v)
        }
        if (receiveMaximum.isSome()) {
            let v = receiveMaximum.getOrThrow()
            buf.add(PropId.receiveMaximum); writeUint16(v)
        }
        if (maximumPacketSize.isSome()) {
            let v = maximumPacketSize.getOrThrow()
            buf.add(PropId.maximumPacketSize); writeUint32(v)
        }
        if (topicAliasMaximum.isSome()) {
            let v = topicAliasMaximum.getOrThrow()
            buf.add(PropId.topicAliasMaximum); writeUint16(v)
        }
        if (topicAlias.isSome()) {
            let v = topicAlias.getOrThrow()
            buf.add(PropId.topicAlias); writeUint16(v)
        }

        // 字节属性
        if (requestResponseInformation.isSome()) {
            let v = requestResponseInformation.getOrThrow()
            buf.add(PropId.requestResponseInformation)
            buf.add(if (v) { UInt8(1) } else { UInt8(0) })
        }
        if (requestProblemInformation.isSome()) {
            let v = requestProblemInformation.getOrThrow()
            buf.add(PropId.requestProblemInformation)
            buf.add(if (v) { UInt8(1) } else { UInt8(0) })
        }
        if (payloadFormatIndicator.isSome()) {
            let v = payloadFormatIndicator.getOrThrow()
            buf.add(PropId.payloadFormatIndicator)
            buf.add(if (v) { UInt8(1) } else { UInt8(0) })
        }

        if (messageExpiryInterval.isSome()) {
            let v = messageExpiryInterval.getOrThrow()
            buf.add(PropId.messageExpiryInterval); writeUint32(v)
        }
        if (subscriptionIdentifier.isSome()) {
            let v = subscriptionIdentifier.getOrThrow()
            buf.add(PropId.subscriptionIdentifier); writeVbi(v)
        }
        if (responseTopic.isSome()) {
            let v = responseTopic.getOrThrow()
            buf.add(PropId.responseTopic); writeUtf8(v)
        }
        if (correlationData.isSome()) {
            let v = correlationData.getOrThrow()
            buf.add(PropId.correlationData); writeBinary(v)
        }
        if (contentType.isSome()) {
            let v = contentType.getOrThrow()
            buf.add(PropId.contentTyp); writeUtf8(v)
        }
        if (willDelayInterval.isSome()) {
            let v = willDelayInterval.getOrThrow()
            buf.add(PropId.willDelayInterval); writeUint32(v)
        }

        // User Properties
        for ((k, v) in userProperties) {
            buf.add(PropId.userProperty); writeUtf8(k); writeUtf8(v)
        }

        // Reason String (0x1F) — DISCONNECT / AUTH / QoS ack 均可携带
        if (reasonString.isSome()) {
            let v = reasonString.getOrThrow()
            buf.add(PropId.reasonString); writeUtf8(v)
        }
        // Authentication Method (0x15) and Data (0x16) — v5.0 增强认证
        if (authenticationMethod.isSome()) {
            let v = authenticationMethod.getOrThrow()
            buf.add(PropId.authenticationMethod); writeUtf8(v)
        }
        if (authenticationData.isSome()) {
            let v = authenticationData.getOrThrow()
            buf.add(PropId.authenticationData); writeBinary(v)
        }

        // 转换为 Array<UInt8>
        let result = Array<UInt8>(buf.size, repeat: UInt8(0))
        for (i in 0..buf.size) { result[Int64(i)] = buf[Int64(i)] }
        return result
    }

    // 是否为空的快捷判断
    public func isEmpty(): Bool {
    return sessionExpiryInterval.isNone() && receiveMaximum.isNone() &&
        maximumPacketSize.isNone() && topicAliasMaximum.isNone() &&
        requestResponseInformation.isNone() && requestProblemInformation.isNone() &&
        payloadFormatIndicator.isNone() && messageExpiryInterval.isNone() &&
        responseTopic.isNone() && correlationData.isNone() &&
        subscriptionIdentifier.isNone() && topicAlias.isNone() &&
        contentType.isNone() && willDelayInterval.isNone() &&
        serverKeepAlive.isNone() && assignedClientIdentifier.isNone() &&
        serverReference.isNone() && responseInformation.isNone() &&
        reasonString.isNone() && authenticationMethod.isNone() &&
        authenticationData.isNone() && maximumQoS.isNone() &&
        retainAvailable.isNone() && wildcardSubscriptionAvailable.isNone() &&
        subscriptionIdentifierAvailable.isNone() && sharedSubscriptionAvailable.isNone() &&
        userProperties.isEmpty()
    }

    // ---- 解码辅助 ----

    /// 从字节数组解码 Properties
    ///
    /// @param data Properties 部分的原始字节（不含长度前缀）
    /// @return 解析后的 MqttProperties
    public static func decode(data: Array<UInt8>): MqttProperties {
        let props = MqttProperties()
        var i: Int64 = 0
        while (i < data.size) {
            let id = data[i]
            i += 1
            match (id) {
                case 0x01 =>
                    props.payloadFormatIndicator = Some(data[i] != 0)
                    i += 1
                case 0x02 =>
                    props.messageExpiryInterval = Some(readUint32(data, i))
                    i += 4
                case 0x08 =>
                    let (s, n) = readUtf8(data, i)
                    props.responseTopic = Some(s)
                    i += n
                case 0x09 =>
                    let (buf, n) = readBinary(data, i)
                    props.correlationData = Some(buf)
                    i += n
                case 0x0B =>
                    let (v, n) = readVbi(data, i)
                    props.subscriptionIdentifier = Some(v)
                    i += n
                case 0x03 =>
                    let (s, n) = readUtf8(data, i)
                    props.contentType = Some(s)
                    i += n
                case 0x18 =>
                    props.willDelayInterval = Some(readUint32(data, i))
                    i += 4
                case 0x11 =>
                    props.sessionExpiryInterval = Some(readUint32(data, i))
                    i += 4
                case 0x13 =>
                    props.serverKeepAlive = Some(Int64(readUint16(data, i)))
                    i += 2
                case 0x15 =>
                    let (s, n) = readUtf8(data, i)
                    props.authenticationMethod = Some(s)
                    i += n
                case 0x16 =>
                    let (buf, n) = readBinary(data, i)
                    props.authenticationData = Some(buf)
                    i += n
                case 0x1A =>
                    props.requestProblemInformation = Some(data[i] != 0)
                    i += 1
                case 0x19 =>
                    props.requestResponseInformation = Some(data[i] != 0)
                    i += 1
                case 0x1F =>
                    let (s, n) = readUtf8(data, i)
                    props.reasonString = Some(s)
                    i += n
                case 0x21 =>
                    props.receiveMaximum = Some(Int64(readUint16(data, i)))
                    i += 2
                case 0x22 =>
                    props.topicAliasMaximum = Some(Int64(readUint16(data, i)))
                    i += 2
                case 0x23 =>
                    props.topicAlias = Some(Int64(readUint16(data, i)))
                    i += 2
                case 0x27 =>
                    props.maximumPacketSize = Some(readUint32(data, i))
                    i += 4
                case 0x24 =>
                    props.maximumQoS = Some(Int64(data[i]))
                    i += 1
                case 0x25 =>
                    props.retainAvailable = Some(data[i] != 0)
                    i += 1
                case 0x28 =>
                    props.wildcardSubscriptionAvailable = Some(data[i] != 0)
                    i += 1
                case 0x29 =>
                    props.subscriptionIdentifierAvailable = Some(data[i] != 0)
                    i += 1
                case 0x2A =>
                    props.sharedSubscriptionAvailable = Some(data[i] != 0)
                    i += 1
                case 0x12 =>
                    let (s, n) = readUtf8(data, i)
                    props.assignedClientIdentifier = Some(s)
                    i += n
                case 0x1C =>
                    let (s, n) = readUtf8(data, i)
                    props.serverReference = Some(s)
                    i += n
                case 0x1B =>
                    let (s, n) = readUtf8(data, i)
                    props.responseInformation = Some(s)
                    i += n
                case 0x26 =>
                    let (k, n1) = readUtf8(data, i)
                    i += n1
                    let (v, n2) = readUtf8(data, i)
                    i += n2
                    props.userProperties.add(k, v)
                case _ =>
                    i = data.size
            }
        }
        return props
    }

    private static func readUint16(data: Array<UInt8>, offset: Int64): UInt16 {
        return (UInt16(data[offset]) << 8) | UInt16(data[offset + 1])
    }

    private static func readUint32(data: Array<UInt8>, offset: Int64): Int64 {
        return (Int64(data[offset]) << 24) | (Int64(data[offset + 1]) << 16) |
               (Int64(data[offset + 2]) << 8) | Int64(data[offset + 3])
    }

    private static func readUtf8(data: Array<UInt8>, offset: Int64): (String, Int64) {
        let len = Int64(readUint16(data, offset))
        let arr = Array<UInt8>(len, repeat: 0)
        for (j in 0..len) {
            arr[Int64(j)] = data[offset + 2 + j]
        }
        return (String.fromUtf8(arr), 2 + len)
    }

    private static func readBinary(data: Array<UInt8>, offset: Int64): (Array<UInt8>, Int64) {
        let len = Int64(readUint16(data, offset))
        let arr = Array<UInt8>(len, repeat: 0)
        for (j in 0..len) {
            arr[Int64(j)] = data[offset + 2 + j]
        }
        return (arr, 2 + len)
    }

    private static func readVbi(data: Array<UInt8>, offset: Int64): (Int64, Int64) {
        var value: Int64 = 0
        var multiplier: Int64 = 1
        var pos = offset
        var byte: UInt8
        var byteCount: Int64 = 0
        do {
            if (pos >= data.size) {
                return (value, pos - offset)  // 数据边界保护
            }
            byte = data[pos]
            value += Int64(byte & 0x7F) * multiplier
            multiplier *= 128
            pos += 1
            byteCount += 1
            if (byteCount > 4) {
                throw ProtocolError("VBI 超过 4 字节上限（非法格式）")
            }
        } while ((byte & 0x80) != 0)
        if (value > 268435455) {
            throw ProtocolError("VBI 值超出 MQTT 上限 268435455: ${value}")
        }
        return (value, pos - offset)
    }

    // ---- 内部编码辅助 ----
    private static func writeUint16(buf: ByteArrayOutputStream, v: Int64): Unit {
        buf.write(UInt8((v >> 8) & 0xFF))
        buf.write(UInt8(v & 0xFF))
    }

    private static func writeUint32(buf: ByteArrayOutputStream, v: Int64): Unit {
        buf.write(UInt8((v >> 24) & 0xFF))
        buf.write(UInt8((v >> 16) & 0xFF))
        buf.write(UInt8((v >> 8) & 0xFF))
        buf.write(UInt8(v & 0xFF))
    }

    private static func writeVbi(buf: ByteArrayOutputStream, v: Int64): Unit {
        if (v < 0 || v > 268435455) {
            throw ProtocolError("VBI 值超出合法范围: ${v}")
        }
        var val = v
        do {
            var byte = UInt8(val % 128)
            val = val / 128
            if (val > 0) { byte = byte | 0x80 }
            buf.write(byte)
        } while (val > 0)
    }

    private static func writeUtf8(buf: ByteArrayOutputStream, s: String): Unit {
        let bytes = encodeUtf8(s)
        let arr = Array<UInt8>(bytes.size, repeat: UInt8(0))
        for (i in 0..bytes.size) { arr[Int64(i)] = bytes[Int64(i)] }
        writeUint16(buf, Int64(arr.size))
        buf.writeAll(arr)
    }

    /// 将 String 编码为 UTF-8 字节（支持全 Unicode 范围，含 4 字节补充平面）
    private static func encodeUtf8(s: String): ArrayList<UInt8> {
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
        return result
    }

    private static func writeBinary(buf: ByteArrayOutputStream, data: Array<UInt8>): Unit {
        writeUint16(buf, Int64(data.size))
        buf.writeAll(data)
    }
}

// v5.0 Reason Codes（用于 CONNACK/PUBACK/SUBACK/DISCONNECT 等）
public enum ReasonCode {
    | Success
    | NormalDisconnection
    | DisconnectWithWillMessage
    | NoMatchingSubscribers
    | NoSubscriptionExisted
    | ContinueAuthentication
    | ReAuthenticate
    | UnspecifiedError
    | MalformedPacket
    | ProtocolError
    | ImplementationSpecificError
    | UnsupportedProtocolVersion
    | ClientIdentifierNotValid
    | BadUserNameOrPassword
    | NotAuthorized
    | ServerUnavailable
    | ServerBusy
    | Banned
    | ServerShuttingDown
    | BadAuthenticationMethod
    | KeepAliveTimeout
    | SessionTakenOver
    | TopicFilterInvalid
    | TopicNameInvalid
    | PacketIdentifierInUse
    | PacketIdentifierNotFound
    | ReceiveMaximumExceeded
    | TopicAliasInvalid
    | PacketTooLarge
    | MessageRateTooHigh
    | QuotaExceeded
    | AdministrativeAction
    | PayloadFormatInvalid
    | RetainNotSupported
    | QoSNotSupported
    | UseAnotherServer
    | ServerMoved
    | SharedSubscriptionsNotSupported
    | ConnectionRateExceeded
    | MaximumConnectTime
    | SubscriptionIdentifiersNotSupported
    | WildcardSubscriptionsNotSupported

    public func toInt(): Int64 {
        match (this) {
            case Success => 0
            case NormalDisconnection => 0
            case DisconnectWithWillMessage => 4
            case NoMatchingSubscribers => 16
            case NoSubscriptionExisted => 17
            case ContinueAuthentication => 24
            case ReAuthenticate => 25
            case UnspecifiedError => 128
            case MalformedPacket => 129
            case ProtocolError => 130
            case ImplementationSpecificError => 131
            case UnsupportedProtocolVersion => 132
            case ClientIdentifierNotValid => 133
            case BadUserNameOrPassword => 134
            case NotAuthorized => 135
            case ServerUnavailable => 136
            case ServerBusy => 137
            case Banned => 138
            case ServerShuttingDown => 139
            case BadAuthenticationMethod => 140
            case KeepAliveTimeout => 141
            case SessionTakenOver => 142
            case TopicFilterInvalid => 143
            case TopicNameInvalid => 144
            case PacketIdentifierInUse => 145
            case PacketIdentifierNotFound => 146
            case ReceiveMaximumExceeded => 147
            case TopicAliasInvalid => 148
            case PacketTooLarge => 149
            case MessageRateTooHigh => 150
            case QuotaExceeded => 151
            case AdministrativeAction => 152
            case PayloadFormatInvalid => 153
            case RetainNotSupported => 154
            case QoSNotSupported => 155
            case UseAnotherServer => 156
            case ServerMoved => 157
            case SharedSubscriptionsNotSupported => 158
            case ConnectionRateExceeded => 159
            case MaximumConnectTime => 160
            case SubscriptionIdentifiersNotSupported => 161
            case WildcardSubscriptionsNotSupported => 162
        }
    }

    public static func fromInt(value: Int64): ReasonCode {
        match (value) {
            case 0 => Success
            case 4 => DisconnectWithWillMessage
            case 16 => NoMatchingSubscribers
            case 17 => NoSubscriptionExisted
            case 24 => ContinueAuthentication
            case 25 => ReAuthenticate
            case 128 => UnspecifiedError
            case 129 => MalformedPacket
            case 130 => ProtocolError
            case 131 => ImplementationSpecificError
            case 132 => UnsupportedProtocolVersion
            case 133 => ClientIdentifierNotValid
            case 134 => BadUserNameOrPassword
            case 135 => NotAuthorized
            case 136 => ServerUnavailable
            case 137 => ServerBusy
            case 138 => Banned
            case 139 => ServerShuttingDown
            case 140 => BadAuthenticationMethod
            case 141 => KeepAliveTimeout
            case 142 => SessionTakenOver
            case 143 => TopicFilterInvalid
            case 144 => TopicNameInvalid
            case 145 => PacketIdentifierInUse
            case 146 => PacketIdentifierNotFound
            case 147 => ReceiveMaximumExceeded
            case 148 => TopicAliasInvalid
            case 149 => PacketTooLarge
            case 150 => MessageRateTooHigh
            case 151 => QuotaExceeded
            case 152 => AdministrativeAction
            case 153 => PayloadFormatInvalid
            case 154 => RetainNotSupported
            case 155 => QoSNotSupported
            case 156 => UseAnotherServer
            case 157 => ServerMoved
            case 158 => SharedSubscriptionsNotSupported
            case 159 => ConnectionRateExceeded
            case 160 => MaximumConnectTime
            case 161 => SubscriptionIdentifiersNotSupported
            case 162 => WildcardSubscriptionsNotSupported
            case _ => UnspecifiedError
        }
    }
}

// Transport 接口（支持 TCP、TLS、WebSocket 等不同传输层）
public interface Transport {
    func connect(): Result<Unit, String>
    func send(data: Array<Byte>): Result<Int, String>
    func onReceive(callback: (Array<Byte>) -> Unit): Unit
    func close(): Unit
    func isConnected(): Bool
}

public class MqttPacket {
    public var packetType: PacketType
    public var flags: UInt8
    public var remainingLength: Int64
    public var payload: Array<UInt8>
    
    public init(packetType: PacketType, flags: UInt8, payload: Array<UInt8>) {
        this.packetType = packetType
        this.flags = flags
        this.remainingLength = payload.size
        this.payload = payload
    }
}

// 连接选项
public class ConnectOptions {
    public var clientId: String
    public var host: String
    public var port: Int
    public var protocolVersion: ProtocolVersion = ProtocolVersion.V3_1_1  // 默认 v3.1.1
    public var cleanStart: Bool
    public var keepAlive: Int
    public var username: ?String
    public var password: ?Array<UInt8>
    public var willTopic: ?String
    public var willPayload: ?Array<UInt8>
    public var willQos: QoS
    public var willRetain: Bool
    public var willProperties: MqttProperties = MqttProperties()  // v5.0 Will Properties（Will Delay / Content Type 等）
    public var useTls: Bool = false
    public var tlsOptions: TlsOptions = TlsOptions()
    public var properties: MqttProperties = MqttProperties()
    public var autoReconnect: Bool = false
    public var maxReconnectAttempts: Int64 = 5
    public var reconnectBaseDelayMs: Int64 = 1000
    public var useWs: Bool = false
    public var wsPath: String = "/mqtt"
    
    public init(clientId: String, host: String, port!: Int = 1883) {
        this.clientId = clientId
        this.host = host
        this.port = port
        this.cleanStart = true
        this.keepAlive = 60
        this.username = None
        this.password = None
        this.willTopic = None
        this.willPayload = None
        this.willQos = QoS.AT_MOST_ONCE
        this.willRetain = false
        this.useTls = false
        this.tlsOptions = TlsOptions()
    }
}

// 订阅主题
public class Subscription {
    public var topic: String
    public var qos: QoS
    
    public init(topic: String, qos!: QoS = QoS.AT_MOST_ONCE) {
        this.topic = topic
        this.qos = qos
    }
}

// 接收到的消息
public class MqttMessage {
    public var topic: String
    public var payload: Array<UInt8>
    public var qos: QoS
    public var retained: Bool
    public var packetId: ?Int
    
    public init(topic: String, payload: Array<UInt8>, qos: QoS, retained: Bool, packetId!: ?Int = None) {
        this.topic = topic
        this.payload = payload
        this.qos = qos
        this.retained = retained
        this.packetId = packetId
    }
}

