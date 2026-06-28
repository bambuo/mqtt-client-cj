// MQTT 协议解码器
//
// 通用包解析器（版本无关）：读取 Fixed Header + Remaining Length，返回 MqttPacket。
// 版本特定的解析方法已迁移至 ProtocolCodec 策略体系：
// - ProtocolCodec.create(version).parseConnAck(packet)
// - ProtocolCodec.create(version).parsePublish(packet)
// - ...
//
// 支持 TCP 分包/粘包：通过 getPacketSize() 检查一个完整包所需的字节数，
// 调用方自行维护缓冲区，累积足够数据后再 decode()。

package mqtt.protocol

public class MqttDecoder {

    /// 计算一个完整 MQTT 包的总字节数
    ///
    /// 如果数据不足以确定包大小，返回 None。
    /// 可用于 TCP 分包场景：调用方累积数据至 size 足够后，再执行 decode。
    public static func getPacketSize(data: Array<UInt8>): ?Int64 {
        if (data.size < 2) {
            return None
        }
        // 读取可变字节整数（从位置 1 开始）以计算总大小
        var vbiValue: Int64 = 0
        var multiplier: Int64 = 1
        var vbiLen: Int64 = 0
        var pos: Int64 = 1
        var byte: Int64
        var byteCount: Int64 = 0
        do {
            if (pos >= data.size) {
                return None  // VBI 不完整
            }
            byte = Int64(data[pos])
            vbiValue += (byte & 0x7F) * multiplier
            multiplier *= 128
            pos += 1
            byteCount += 1
            if (byteCount > 4) {
                return None  // 超过 4 字节，非法 VBI
            }
        } while ((byte & 0x80) != 0)

        // MQTT 规范上限：VBI 值 ≤ 268,435,455
        if (vbiValue > 268435455) {
            return None  // 非法 VBI
        }

        let totalSize: Int64 = 1 + byteCount + vbiValue
        if (data.size < totalSize) {
            return None  // Payload 不完整
        }
        return Some(totalSize)
    }

    /// 解码接收到的数据（版本无关的通用解析）
    ///
    /// 解析 Fixed Header 和 Remaining Length，将剩余字节作为 payload 返回。
    /// 版本特定的包解析由 ProtocolCodec 子类完成。
    /// 入参 data 必须为一个完整的 MQTT 包（不完整时返回 None）。
    public static func decode(data: Array<UInt8>): ?MqttPacket {
        let sizeOpt = getPacketSize(data)
        if (sizeOpt.isNone()) {
            return None
        }

        let input = ByteArrayInputStream(data)

        // 读取 Fixed Header
        let firstByte = input.read()
        let packetTypeValue = (firstByte & 0xF0) >> 4
        // MQTT 协议包类型为 1-14（15=AUTH 仅 v5.0）
        if (packetTypeValue < UInt8(1) || packetTypeValue > UInt8(15)) {
            return None  // 非法包类型
        }
        let packetType = PacketType.fromInt(Int64(packetTypeValue))
        let flags = firstByte & 0x0F

        // 校验固定头标志位（MQTT v3.1.1 Section 2.2.2）
        if (!validateFixedHeaderFlags(packetType, flags)) {
            return None  // 标志位非法
        }

        // 读取 Remaining Length (可变字节整数)
        let remainingLength = readVariableByteInteger(input)

        // 检查 payload 是否在可用数据范围内
        if (Int64(input.getPosition()) + remainingLength > Int64(data.size)) {
            return None
        }

        // 读取 payload
        let payload = Array<UInt8>(Int64(remainingLength), repeat: 0)
        for (i in 0..remainingLength) {
            payload[Int64(i)] = input.read()
        }

        return Some(MqttPacket(packetType, UInt8(flags), payload))
    }

    // 读取可变字节整数
    private static func readVariableByteInteger(input: ByteArrayInputStream): Int {
        var result: Int = 0
        var multiplier: Int = 1
        var byteCount: Int = 0

        var byte: Int64 = 0
        do {
            byte = Int64(input.read())
            result = result + (byte & 0x7F) * multiplier
            multiplier = multiplier * 128
            byteCount += 1
            if (byteCount > 4) {
                throw ProtocolError("VBI 超过 4 字节上限（非法格式）")
            }
        } while ((byte & 0x80) != 0)

        // MQTT 规范上限 268,435,455
        if (result > 268435455) {
            throw ProtocolError("VBI 值超出 MQTT 上限 268435455: ${result}")
        }
        return result
    }

    /// 校验固定头标志位是否符合 MQTT 协议规范（v3.1.1 Section 2.2.2）
    private static func validateFixedHeaderFlags(packetType: PacketType, flags: UInt8): Bool {
        match (packetType) {
            case PUBLISH =>
                // PUBLISH 标志位可变，但 QoS=3（bits2-1=11）为保留值；QoS=0 不可设 DUP
                (flags & 0x06) != 0x06 &&
                !((flags & 0x06) == 0 && (flags & 0x08) != 0)
            case PUBREL => (flags & 0x0F) == UInt8(0x02)  // 必须为 0x02
            case SUBSCRIBE => (flags & 0x0F) == UInt8(0x02)  // 必须为 0x02
            case UNSUBSCRIBE => (flags & 0x0F) == UInt8(0x02)  // 必须为 0x02
            case _ => flags == UInt8(0)  // 其他所有包类型 flags 必须为 0
        }
    }
}

// 字节数组输入流
public class ByteArrayInputStream {
    private var data: Array<UInt8>
    private var position: Int = 0

    public init(data: Array<UInt8>) {
        this.data = data
    }

    public func read(): UInt8 {
        if (position >= data.size) {
            throw IndexOutOfBoundsException("ByteArrayInputStream 读取超出边界")
        }
        let byte = data[Int64(position)]
        position = position + 1
        return byte
    }

    public func readShort(): UInt16 {
        let high = UInt16(read())
        let low = UInt16(read())
        return (high << 8) | low
    }

    public func getPosition(): Int {
        return position
    }
}
