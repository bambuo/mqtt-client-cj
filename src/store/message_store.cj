// 消息存储（用于 QoS 1/2）
package mqtt.store

import std.time.*
import std.collection.*

/// Inflight 消息状态
public enum InflightState {
    | SENT       // 已发送，等待确认
    | RECEIVED   // 已收到，等待确认（服务端角度）
    | WAIT_PUBREL  // QoS 2: 已收到 PUBREC，等待 PUBREL

    public func toInt(): Int64 {
        match (this) {
            case SENT => 0
            case RECEIVED => 1
            case WAIT_PUBREL => 2
        }
    }
}

/// Inflight 消息（QoS 1/2 需要）
public class InflightMessage {
    public let packetId: Int64
    public let packetData: Array<UInt8>
    public var state: InflightState
    public var timestamp: Int64  // 发送时间戳（毫秒）
    public var retryCount: Int64 = 0
    
    public init(packetId: Int64, packetData: Array<UInt8>, state: InflightState) {
        this.packetId = packetId
        this.packetData = packetData
        this.state = state
        this.timestamp = getCurrentTimeMillis()
    }
    
    /// 检查是否超时（默认 5 秒）
    public func isTimeout(timeout!: Int64 = 5000): Bool {
        let now = getCurrentTimeMillis()
        return (now - timestamp) > timeout
    }
    
    /// 更新时间戳（用于重传）
    public func updateTimestamp(): Unit {
        this.timestamp = getCurrentTimeMillis()
        this.retryCount = retryCount + 1
    }
}

/// 消息存储（管理 Inflight 消息）
public class MessageStore {
    private var inflightMessages: Map<Int64, InflightMessage> = HashMap()
    private var nextPacketId: Int64 = 1
    
    /// 生成新的 packet ID
    ///
    /// MQTT 规范（§2.3.1 / §2.2.2）要求：packet ID 在 QoS 1/2 消息确认前不得复用。
    /// 因此本方法跳过当前仍 in-flight（存在未确认 QoS1/2 消息）的 ID。
    /// 若所有 ID 都在 in-flight（65535 个未确认），返回 None（调用方应做背压处理）。
    /// 注意：调用方需自行保证并发安全（如外部持锁）。
    public func generatePacketId(): ?Int64 {
        var id = nextPacketId
        var tried = 0
        // 跳过已 in-flight 的 ID（最多尝试 65535 次）
        while (inflightMessages.contains(id) && tried < 65535) {
            id = (id % 65535) + 1
            tried += 1
        }
        if (inflightMessages.contains(id)) {
            return None  // 全部 ID 都在 in-flight
        }
        nextPacketId = (id % 65535) + 1
        return id
    }
    
    /// 存储 inflight 消息
    public func store(packetId: Int64, packetData: Array<UInt8>, state: InflightState): Unit {
        let inflight = InflightMessage(packetId, packetData, state)
        inflightMessages.add(packetId, inflight)
    }
    
    /// 根据 packet ID 获取 inflight 消息
    public func get(packetId: Int64): ?InflightMessage {
        return inflightMessages.get(packetId)
    }
    
    /// 根据 packet ID 删除 inflight 消息
    public func remove(packetId: Int64): ?InflightMessage {
        return inflightMessages.remove(packetId)
    }
    
    /// 更新 inflight 消息状态（QoS 2 四次握手）
    public func updateState(packetId: Int64, newState: InflightState): Bool {
        if (inflightMessages.contains(packetId)) {
            let inflight = inflightMessages[packetId]
            inflight.state = newState
            return true
        }
        return false
    }
    
    /// 获取所有超时的消息
    public func getTimeoutMessages(timeout!: Int64 = 5000): ArrayList<InflightMessage> {
        let result = ArrayList<InflightMessage>()
        
        for ((packetId, inflight) in inflightMessages) {
            if (inflight.isTimeout(timeout: timeout)) {
                result.add(inflight)
            }
        }
        
        return result
    }
    
    /// 清空所有消息
    public func clear(): Unit {
        inflightMessages.clear()
    }
    
    /// 获取当前 inflight 消息数量
    public func size(): Int64 {
        return inflightMessages.size
    }
}

// 辅助函数：获取当前时间戳（毫秒）
// 使用单调时钟，仅用于超时计算，不表示绝对时间
private func getCurrentTimeMillis(): Int64 {
    // 简单方案：返回单调时钟的毫秒数
    // 使用一个全局基准点
    MonoTimeSample.refresh()
    return MonoTimeSample.elapsedMs
}

// 单调时钟辅助（简化实现）
private class MonoTimeSample {
    private static let start = MonoTime.now()
    public static var elapsedMs: Int64 = 0
    
    public static func refresh(): Unit {
        let now = MonoTime.now()
        elapsedMs = (now - start).toMilliseconds()
    }
}
