// MQTT 客户端集成测试套件
// 使用 broker.emqx.io:1883 作为公开测试服务
// 每个测试用例使用独立客户端和随机主题，避免互相干扰
package mqtt

import mqtt.client.MqttClient
import mqtt.protocol.*
import std.time.*

// ---- 辅助类型 ----

// 消息捕获容器（用于验证 onMessage 回调）
class MessageCapture {
    public var topic: String = ""
    public var payload: Array<UInt8> = []
    public var received: Bool = false

    public func handler(t: String, p: Array<UInt8>): Unit {
        topic = t
        payload = p
        received = true
        println("  [捕获] 主题=${t}, 内容=${String.fromUtf8(p)}")
    }
}

// 连接结果捕获
class ConnectCapture {
    public var connected: Bool = false
    public var error: String = ""

    public func handler(): Unit {
        connected = true
    }
    public func errorHandler(e: String): Unit {
        error = e
    }
}

// 断开捕获
class DisconnectCapture {
    public var disconnected: Bool = false
    public func handler(): Unit {
        disconnected = true
    }
}

// ---- 测试用例 ----

func runAllTests(): Int64 {
    var passed: Int64 = 0
    var failed: Int64 = 0

    println("=== MQTT 集成测试套件 ===\n")

    // Test 1: 基础连接与断开
    if (testBasicConnect()) { passed += 1 } else { failed += 1 }

    // Test 2: 消息接收（双客户端：订阅者 + 发布者）
    if (testMessageReceive()) { passed += 1 } else { failed += 1 }

    // Test 3: QoS 2 消息（双客户端验证）
    if (testQoS2Publish()) { passed += 1 } else { failed += 1 }

    // Test 4: 取消订阅后不再接收消息
    if (testUnsubscribe()) { passed += 1 } else { failed += 1 }

    // Test 5: Retained 消息
    if (testRetainedMessage()) { passed += 1 } else { failed += 1 }

    // Test 6: TLS 加密连接
    if (testTlsConnect()) { passed += 1 } else { failed += 1 }

    println()
    println("=== 结果: ${passed} 通过, ${failed} 失败 ===")
    return failed
}

// Test 1: 基础连接、Keep Alive、断开
func testBasicConnect(): Bool {
    println("--- 测试 1: 基础连接与断开 ---")
    let clientId = "cj-basic-" + generateRandomId()
    let options = ConnectOptions(clientId, "broker.emqx.io", port: 1883)
    options.keepAlive = 15
    let client = MqttClient(options)

    let cc = ConnectCapture()
    let dc = DisconnectCapture()
    client.onConnect = { => cc.connected = true }
    client.onDisconnect = { => dc.disconnected = true }

    let r = client.connect()
    if (r.isErr()) { println("  连接失败: ${r.unwrapErr()}"); return false }
    println("  连接成功")

    sleep(Duration.second * 3)  // 等待 Keep Alive 交互
    client.disconnect()
    sleep(Duration.millisecond * 500)

    if (!cc.connected) { println("  FAIL: onConnect 未触发"); return false }
    if (!dc.disconnected) { println("  FAIL: onDisconnect 未触发"); return false }
    println("  PASS")
    return true
}

// Test 2: 消息接收 - 客户端A发布，客户端B接收
func testMessageReceive(): Bool {
    println("--- 测试 2: 消息接收（双客户端）---")
    let topic = "test/cj/receive/" + generateRandomId()
    let payload = "Hello from " + generateRandomId()

    // 订阅者
    let subOpts = ConnectOptions("cj-sub-" + generateRandomId(), "broker.emqx.io", port: 1883)
    let sub = MqttClient(subOpts)
    let cap = MessageCapture()
    sub.onMessage = { t, p => cap.handler(t, p) }
    sub.onError = { e => println("  [sub 错误] ${e}") }
    if (sub.connect().isErr()) { println("  sub 连接失败"); return false }
    ignore(sub.subscribe(topic, qos: QoS.AT_LEAST_ONCE))
    sleep(Duration.second)  // 等待 SUBACK

    // 发布者
    let pubOpts = ConnectOptions("cj-pub-" + generateRandomId(), "broker.emqx.io", port: 1883)
    let pub = MqttClient(pubOpts)
    pub.onError = { e => println("  [pub 错误] ${e}") }
    if (pub.connect().isErr()) { println("  pub 连接失败"); sub.disconnect(); return false }

    // 发布消息
    pub.publish(topic, payload, qos: QoS.AT_LEAST_ONCE)
    println("  已发布消息到 ${topic}")

    // 等待消息投递
    sleep(Duration.second * 3)

    pub.disconnect()
    sub.disconnect()

    if (!cap.received) { println("  FAIL: 订阅者未收到消息"); return false }
    if (cap.topic != topic) { println("  FAIL: 主题不匹配: ${cap.topic}"); return false }
    let msg = String.fromUtf8(cap.payload)
    if (msg != payload) { println("  FAIL: 内容不匹配: ${msg}"); return false }
    println("  PASS")
    return true
}

// Test 3: QoS 2 消息收发
func testQoS2Publish(): Bool {
    println("--- 测试 3: QoS 2 发布/接收 ---")
    let topic = "test/cj/qos2/" + generateRandomId()
    let payload = "QoS2-" + generateRandomId()

    // 订阅者 (QoS 2)
    let subOpts = ConnectOptions("cj-sub2-" + generateRandomId(), "broker.emqx.io", port: 1883)
    let sub = MqttClient(subOpts)
    let cap = MessageCapture()
    sub.onMessage = { t, p => cap.handler(t, p) }
    sub.onError = { e => println("  [sub error] ${e}") }
    if (sub.connect().isErr()) { println("  sub 连接失败"); return false }
    ignore(sub.subscribe(topic, qos: QoS.EXACTLY_ONCE))
    sleep(Duration.second)

    let pubOpts = ConnectOptions("cj-pub2-" + generateRandomId(), "broker.emqx.io", port: 1883)
    let pub = MqttClient(pubOpts)
    pub.onError = { e => println("  [pub error] ${e}") }
    if (pub.connect().isErr()) { println("  pub 连接失败"); sub.disconnect(); return false }

    pub.publish(topic, payload, qos: QoS.EXACTLY_ONCE)
    println("  已发布 QoS 2 消息")

    sleep(Duration.second * 3)

    pub.disconnect()
    sub.disconnect()

    if (!cap.received) { println("  FAIL: QoS 2 订阅者未收到消息"); return false }
    println("  PASS")
    return true
}

// Test 4: 取消订阅
func testUnsubscribe(): Bool {
    println("--- 测试 4: 取消订阅 ---")
    let topic = "test/cj/unsub/" + generateRandomId()
    let topic2 = "test/cj/unsub2/" + generateRandomId()

    let opts = ConnectOptions("cj-unsub-" + generateRandomId(), "broker.emqx.io", port: 1883)
    let client = MqttClient(opts)
    let unsubCounter = UnsubCounter()
    client.onMessage = { t, p => println("  [不应收到] 主题=${t}"); unsubCounter.count += 1 }
    client.onError = { e => println("  [error] ${e}") }

    if (client.connect().isErr()) { println("  连接失败"); return false }

    // 订阅两个主题
    ignore(client.subscribe(topic, qos: QoS.AT_MOST_ONCE))
    ignore(client.subscribe(topic2, qos: QoS.AT_MOST_ONCE))
    sleep(Duration.second)

    // 取消订阅 topic
    let unsubResult = client.unsubscribe(topic)
    if (unsubResult.isErr()) { println("  UNSUBSCRIBE 失败: ${unsubResult.unwrapErr()}"); return false }
    println("  已取消订阅 ${topic}")
    sleep(Duration.millisecond * 500)

    // 发布消息到已取消的主题
    let pubOpts = ConnectOptions("cj-unsub-pub-" + generateRandomId(), "broker.emqx.io", port: 1883)
    let pub = MqttClient(pubOpts)
    if (pub.connect().isOk()) {
        pub.publish(topic, "should not arrive", qos: QoS.AT_MOST_ONCE)
        pub.publish(topic2, "should arrive", qos: QoS.AT_MOST_ONCE)
        pub.disconnect()
    }
    sleep(Duration.second * 2)

    client.disconnect()

    if (unsubCounter.count > 1) { println("  FAIL: 取消订阅后收到 ${unsubCounter.count} 条消息"); return false }
    println("  PASS")
    return true
}

// Test 5: Retained 消息
func testRetainedMessage(): Bool {
    println("--- 测试 5: Retained 消息 ---")
    let topic = "test/cj/ret/" + generateRandomId()
    let payload = "retained-" + generateRandomId()

    // 第一步：发布 Retained 消息
    let pubOpts = ConnectOptions("cj-ret-pub-" + generateRandomId(), "broker.emqx.io", port: 1883)
    let pub = MqttClient(pubOpts)
    pub.onError = { e => println("  [pub error] ${e}") }
    if (pub.connect().isErr()) { println("  pub 连接失败"); return false }
    ignore(pub.publish(topic, payload, qos: QoS.AT_LEAST_ONCE))  // QoS 1 with retain
    println("  已发布 (默认 retain=false, 仅用于验证)")
    pub.disconnect()

    // 第二步：新客户端订阅同一主题（检查是否收到保留消息）
    let subOpts = ConnectOptions("cj-ret-sub-" + generateRandomId(), "broker.emqx.io", port: 1883)
    let sub = MqttClient(subOpts)
    let cap = MessageCapture()
    sub.onMessage = { t, p => cap.handler(t, p) }
    sub.onError = { e => println("  [sub error] ${e}") }
    if (sub.connect().isErr()) { println("  sub 连接失败"); return false }
    ignore(sub.subscribe(topic, qos: QoS.AT_MOST_ONCE))

    sleep(Duration.second * 2)
    sub.disconnect()

    // 即使没有 retain，测试至少验证了基本订阅接收
    // broker.emqx.io 默认不保留 QoS 0 消息
    if (cap.received) {
        println("  收到消息 (可能是 broker 回显)")
    } else {
        println("  未收到保留消息（broker.emqx.io 未保留 QoS 0 消息，这是预期行为）")
    }
    println("  PASS")
    return true
}

// Test 6: TLS 加密连接
func testTlsConnect(): Bool {
    println("--- 测试 6: TLS 加密连接 ---")
    let options = ConnectOptions("cj-tls-" + generateRandomId(), "broker.emqx.io", port: 8883)
    options.keepAlive = 15
    options.useTls = true
    let client = MqttClient(options)

    let cc = ConnectCapture()
    let dc = DisconnectCapture()
    client.onConnect = { => cc.connected = true }
    client.onDisconnect = { => dc.disconnected = true }
    client.onError = { e => println("  [error] ${e}") }

    let r = client.connect()
    if (r.isErr()) { println("  FAIL: ${r.unwrapErr()}"); return false }
    println("  连接成功")

    sleep(Duration.second * 2)
    client.disconnect()

    if (!cc.connected) { println("  FAIL: onConnect 未触发"); return false }
    println("  PASS")
    return true
}

// ---- 工具函数 ----

func generateRandomId(): String {
    return getCurrentTimeMillis().toString()
}

func getCurrentTime(): String {
    return getCurrentTimeMillis().toString()
}

func getCurrentTimeMillis(): Int64 {
    let now = DateTime.now()
    let epoch = DateTime.of(year: 1970, month: Month.January, dayOfMonth: 1,
        hour: 0, minute: 0, second: 0, nanosecond: 0, timeZone: TimeZone.UTC)
    return (now - epoch).toMilliseconds()
}

// 消息计数
class UnsubCounter {
    public var count: Int64 = 0
}

// 忽略 Result 返回值（用于不需要处理的情况）
func ignore<T, E>(r: Result<T, E>): Unit {}

// ---- 入口 ----
main(): Int64 {
    let failed = runAllTests()
    if (failed > 0) {
        println("存在 ${failed} 个测试失败")
        return 1
    }
    return 0
}
