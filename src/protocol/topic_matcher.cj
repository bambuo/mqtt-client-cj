// MQTT 主题通配符匹配器
package mqtt.protocol

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
                // + 匹配一个层级，但必须是非空层级（MQTT 规范要求）
                if (topicParts[topicIndex].isEmpty()) {
                    return false
                }
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
    // - 主题过滤器至少一个字符（MQTT-1.5.4）
    public static func isValidTopicFilter(topicFilter!: String): Bool {
        if (topicFilter.isEmpty()) {
            return false
        }
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
    
    // 验证 topic 是否合法 (不能包含通配符，至少一个字符)
    public static func isValidTopic(topic!: String): Bool {
        // topic 不能包含 # 或 +，且不能为空
        return !topic.contains("#") && !topic.contains("+") && !topic.isEmpty()
    }
}
