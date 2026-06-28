// MQTT 主题通配符匹配器
package mqtt.protocol

import std.convert.*
import std.collection.*

public class TopicMatcher {
    
    // 检查 topic 是否匹配 topicFilter
    // topic: 发布消息的主题 (如 "home/bedroom/temperature")
    // topicFilter: 订阅时使用的主题过滤器 (如 "home/+/temperature" 或 "home/#")
    public static func matches(topic!: String, topicFilter!: String): Bool {
        // $SYS 开头的系统主题不能被通配符匹配
        if (topic.startsWith("$") && !topicFilter.startsWith("$")) {
            return false
        }
        
        let topicParts = topic.split("/")
        let filterParts = topicFilter.split("/")
        
        var topicIndex = 0
        var filterIndex = 0
        
        while (topicIndex < topicParts.size && filterIndex < filterParts.size) {
            let filterPart = filterParts[filterIndex]
            
            if (filterPart == "#") {
                // # 匹配剩余所有层级
                return true
            } else if (filterPart == "+") {
                // + 匹配一个层级
                topicIndex += 1
                filterIndex += 1
            } else {
                // 精确匹配
                if (topicParts[topicIndex] != filterPart) {
                    return false
                }
                topicIndex += 1
                filterIndex += 1
            }
        }
        
        // 检查是否有剩余的 #
        if (filterIndex < filterParts.size && filterParts[filterIndex] == "#") {
            return true
        }
        
        // 必须同时耗尽
        return topicIndex == topicParts.size && filterIndex == filterParts.size
    }
    
    // 验证 topic filter 是否合法
    // 规则:
    // - # 只能出现在末尾
    // - # 前必须是 / 或开头
    // - + 必须单独作为一个层级
    public static func isValidTopicFilter(topicFilter!: String): Bool {
        let parts = topicFilter.split("/")
        
        for (i in 0..parts.size) {
            let part = parts[i]
            
            if (part == "#") {
                // # 只能出现在末尾
                if (i != parts.size - 1) {
                    return false
                }
            } else if (part.contains("#") || part.contains("+")) {
                // # 和 + 必须单独作为一个层级
                if (part != "+") {
                    return false
                }
            }
        }
        
        return true
    }
    
    // 验证 topic 是否合法 (不能包含通配符)
    public static func isValidTopic(topic!: String): Bool {
        // topic 不能包含 # 或 +
        return !topic.contains("#") && !topic.contains("+")
    }
}

// 单元测试
public func testTopicMatcher(): Unit {
    println("=== TopicMatcher 测试 ===")
    
    // 测试 1: 精确匹配
    assert(TopicMatcher.matches(topic: "home/temp", topicFilter: "home/temp"), "精确匹配失败")
    
    // 测试 2: + 通配符
    assert(TopicMatcher.matches(topic: "home/bedroom/temp", topicFilter: "home/+/temp"), "单级通配符失败")
    assert(!TopicMatcher.matches(topic: "home/bedroom/1/temp", topicFilter: "home/+/temp"), "+ 匹配了多级")
    
    // 测试 3: # 通配符
    assert(TopicMatcher.matches(topic: "home/temp", topicFilter: "home/#"), "多级通配符失败 (同级)")
    assert(TopicMatcher.matches(topic: "home/bedroom/temp", topicFilter: "home/#"), "多级通配符失败 (多级)")
    assert(TopicMatcher.matches(topic: "home", topicFilter: "home/#"), "多级通配符失败 (零级)")
    
    // 测试 4: 复杂匹配
    assert(TopicMatcher.matches(topic: "a/b/c/d", topicFilter: "a/+/c/+"), "复杂匹配失败")
    
    // 测试 5: 不匹配的情况
    assert(!TopicMatcher.matches(topic: "home/temp", topicFilter: "home/light"), "不应该匹配")
    assert(!TopicMatcher.matches(topic: "home/bedroom/light", topicFilter: "home/+/temp"), "QoS 不匹配")
    
    // 测试 6: 验证 topic filter
    assert(TopicMatcher.isValidTopicFilter(topicFilter: "home/+"), "合法 filter")
    assert(TopicMatcher.isValidTopicFilter(topicFilter: "home/#"), "合法 filter")
    assert(!TopicMatcher.isValidTopicFilter(topicFilter: "home/#/temp"), "# 不在末尾")
    assert(!TopicMatcher.isValidTopicFilter(topicFilter: "home/a#"), "# 不在单独层级")
    
    println("✅ 所有测试通过!")
}

// 断言辅助函数
private func assert(condition: Bool, message: String): Unit {
    if (!condition) {
        println("测试失败: ${message}")
    }
}
