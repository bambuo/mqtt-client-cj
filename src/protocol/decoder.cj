// MQTT 协议解码器
//
// 通用包解析器（版本无关）：读取 Fixed Header + Remaining Length，返回 MqttPacket。
// 版本特定的解析方法已迁移至 ProtocolCodec 策略体系：
// - ProtocolCodec.create(version).parseConnAck(packet)
// - ProtocolCodec.create(version).parsePublish(packet)
// - ...

package mqtt.protocol

public class MqttDecoder {

    /// 解码接收到的数据（版本无关的通用解析）
    ///
    /// 解析 Fixed Header 和 Remaining Length，将剩余字节作为 payload 返回。
    /// 版本特定的包解析由 ProtocolCodec 子类完成。
    public static func decode(data: Array<UInt8>): ?MqttPacket {
        if (data.size < 2) {
            return None
        }

        let input = ByteArrayInputStream(data)

        // 读取 Fixed Header
        let firstByte = input.read()
        let packetType = PacketType.fromInt(Int64((firstByte & 0xF0) >> 4))
        let flags = firstByte & 0x0F

        // 读取 Remaining Length (可变字节整数)
        let remainingLength = readVariableByteInteger(input)

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

        var byte: Int64 = 0
        do {
            byte = Int64(input.read())
            result = result + (byte & 0x7F) * multiplier
            multiplier = multiplier * 128
        } while ((byte & 0x80) != 0)

        return result
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
