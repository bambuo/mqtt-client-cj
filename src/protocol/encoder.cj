// MQTT 协议编码器（已废弃）
//
// MqttEncoder 已迁移至 ProtocolCodec 策略体系：
// - V311Codec: v3.1.1 编解码
// - V50Codec:   v5.0 编解码
// - 使用方式: ProtocolCodec.create(version).encodeXxx(...)
//
// 本文件仅保留 ByteArrayOutputStream 供包内其他模块使用。

package mqtt.protocol

import std.collection.*

/// 字节数组输出流（包内共享工具）
public class ByteArrayOutputStream {
    private var data = ArrayList<UInt8>()

    public func write(byte: UInt8): Unit {
        data.add(byte)
    }

    public func writeShort(value: UInt16): Unit {
        let high = UInt8((value >> 8) & 0xFF)
        let low = UInt8(value & 0xFF)
        data.add(high)
        data.add(low)
    }

    public func writeAll(bytes: Array<UInt8>): Unit {
        for (b in bytes) {
            data.add(b)
        }
    }

    public func size(): Int64 {
        return data.size
    }

    public func toByteArray(): Array<UInt8> {
        let sz = data.size
        let result = Array<UInt8>(sz, repeat: 0)
        for (i in Int64(0)..sz) {
            result[i] = data[i]
        }
        return result
    }
}
