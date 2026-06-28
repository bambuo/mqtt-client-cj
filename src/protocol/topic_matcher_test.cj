// TopicMatcher 单元测试
package mqtt.protocol

@Test
func testExactMatch() {
    @Expect(TopicMatcher.matches(topic: "home/temp", topicFilter: "home/temp"), true)
}

@Test
func testSingleLevelWildcard() {
    @Expect(TopicMatcher.matches(topic: "home/bedroom/temp", topicFilter: "home/+/temp"), true)
}

@Test
func testSingleLevelWildcardNoMatch() {
    @Expect(TopicMatcher.matches(topic: "home/bedroom/1/temp", topicFilter: "home/+/temp"), false)
}

@Test
func testMultiLevelWildcard() {
    @Expect(TopicMatcher.matches(topic: "home/temp", topicFilter: "home/#"), true)
    @Expect(TopicMatcher.matches(topic: "home/bedroom/temp", topicFilter: "home/#"), true)
    @Expect(TopicMatcher.matches(topic: "home", topicFilter: "home/#"), true)
}

@Test
func testWildcardOnly() {
    @Expect(TopicMatcher.matches(topic: "any/topic/here", topicFilter: "#"), true)
    @Expect(TopicMatcher.matches(topic: "single", topicFilter: "+"), true)
}

@Test
func testComplexWildcard() {
    @Expect(TopicMatcher.matches(topic: "a/b/c/d", topicFilter: "a/+/c/+"), true)
}

@Test
func testNoMatch() {
    @Expect(TopicMatcher.matches(topic: "home/temp", topicFilter: "home/light"), false)
    @Expect(TopicMatcher.matches(topic: "home/bedroom/light", topicFilter: "home/+/temp"), false)
}

@Test
func testSysTopicNoWildcardMatch() {
    // $SYS topics should NOT match wildcards starting without $
    @Expect(TopicMatcher.matches(topic: "$SYS/broker/uptime", topicFilter: "#"), false)
    @Expect(TopicMatcher.matches(topic: "$SYS/broker/clients", topicFilter: "+"), false)
    @Expect(TopicMatcher.matches(topic: "$SYS/broker", topicFilter: "$SYS/broker"), true)
    @Expect(TopicMatcher.matches(topic: "$SYS/broker", topicFilter: "$SYS/#"), true)
}

@Test
func testValidTopicFilter() {
    @Expect(TopicMatcher.isValidTopicFilter(topicFilter: "home/+"), true)
    @Expect(TopicMatcher.isValidTopicFilter(topicFilter: "home/#"), true)
    @Expect(TopicMatcher.isValidTopicFilter(topicFilter: "home/+/+"), true)
    @Expect(TopicMatcher.isValidTopicFilter(topicFilter: "#"), true)
    @Expect(TopicMatcher.isValidTopicFilter(topicFilter: "+"), true)
}

@Test
func testInvalidTopicFilter() {
    @Expect(TopicMatcher.isValidTopicFilter(topicFilter: "home/#/temp"), false)
    @Expect(TopicMatcher.isValidTopicFilter(topicFilter: "home/a#"), false)
    @Expect(TopicMatcher.isValidTopicFilter(topicFilter: "home/a+"), false)
}

@Test
func testIsValidTopic() {
    @Expect(TopicMatcher.isValidTopic(topic: "home/temp"), true)
    @Expect(TopicMatcher.isValidTopic(topic: "test/topic/1"), true)
}

@Test
func testIsInvalidTopic() {
    @Expect(TopicMatcher.isValidTopic(topic: "test/+"), false)
    @Expect(TopicMatcher.isValidTopic(topic: "test/#"), false)
    @Expect(TopicMatcher.isValidTopic(topic: ""), false)       // 空串拒绝
}

// ============ 空层级 / 空串拒绝测试 ============

@Test
func testPlusRejectsEmptyTopicLevel() {
    // + 不应匹配空层级（MQTT 规范要求 + 匹配恰好一个非空层级）
    @Expect(TopicMatcher.matches(topic: "a//c", topicFilter: "a/+/c"), false)
    @Expect(TopicMatcher.matches(topic: "a/b/c", topicFilter: "a/+/c"), true)
}

@Test
func testEmptyTopicFilterRejected() {
    @Expect(TopicMatcher.isValidTopicFilter(topicFilter: ""), false)
}

@Test
func testEmptyTopicRejected() {
    @Expect(TopicMatcher.isValidTopic(topic: ""), false)
}
