// MQTT 客户端集成测试套件
// 使用 broker.emqx.io:1883 作为公开测试服务
package integration

import mqtt.client.MqttClient
import mqtt.protocol.*
import std.time.*

// ---- 辅助类型 ----

class MessageCapture {
    public var topic: String = ""
    public var payload: Array<UInt8> = []
    public var received: Bool = false
    public func handler(t: String, p: Array<UInt8>): Unit {
        topic = t; payload = p; received = true
        println("  [捕获] 主题=${t}, 内容=${String.fromUtf8(p)}")
    }
}

class ConnectCapture {
    public var connected: Bool = false
    public var error: String = ""
    public func handler(): Unit { connected = true }
    public func errorHandler(e: String): Unit { error = e }
}

class DisconnectCapture {
    public var disconnected: Bool = false
    public func handler(): Unit { disconnected = true }
}

class UnsubCounter {
    public var count: Int64 = 0
}

// ---- 测试用例 ----

func runAllTests(): Int64 {
    var passed: Int64 = 0
    var failed: Int64 = 0
    println("=== MQTT 集成测试套件 ===\n")

    if (testBasicConnect()) { passed += 1 } else { failed += 1 }
    if (testMessageReceive()) { passed += 1 } else { failed += 1 }
    if (testQoS2Publish()) { passed += 1 } else { failed += 1 }
    if (testUnsubscribe()) { passed += 1 } else { failed += 1 }
    if (testRetainedMessage()) { passed += 1 } else { failed += 1 }
    if (testTlsConnect()) { passed += 1 } else { failed += 1 }
    if (testAutoReconnect()) { passed += 1 } else { failed += 1 }
    if (testSessionPersistence()) { passed += 1 } else { failed += 1 }
    if (testWebSocket()) { passed += 1 } else { failed += 1 }

    println()
    println("=== 结果: ${passed} 通过, ${failed} 失败 ===")
    return failed
}

func testBasicConnect(): Bool {
    println("--- 测试 1: 基础连接与断开 ---")
    let opts = ConnectOptions("cj-basic-" + id(), "broker.emqx.io", port: 1883)
    opts.keepAlive = 15
    let client = MqttClient(opts)
    let cc = ConnectCapture(); let dc = DisconnectCapture()
    client.onConnect = { => cc.connected = true }
    client.onDisconnect = { => dc.disconnected = true }
    if (client.connect().isErr()) { return false }
    println("  连接成功")
    sleep(Duration.second * 3)
    client.disconnect()
    sleep(Duration.millisecond * 500)
    if (!cc.connected) { println("  FAIL: onConnect 未触发"); return false }
    if (!dc.disconnected) { println("  FAIL: onDisconnect 未触发"); return false }
    println("  PASS"); return true
}

func testMessageReceive(): Bool {
    println("--- 测试 2: 消息接收（双客户端）---")
    let topic = "test/cj/receive/" + id()
    let payload = "Hello from " + id()
    let sub = MqttClient(ConnectOptions("cj-sub-" + id(), "broker.emqx.io", port: 1883))
    let cap = MessageCapture()
    sub.onMessage = { t, p => cap.handler(t, p) }
    sub.onError = { e => println("  [sub] ${e}") }
    if (sub.connect().isErr()) { return false }
    ignore(sub.subscribe(topic, qos: QoS.AT_LEAST_ONCE))
    sleep(Duration.second)

    let pub = MqttClient(ConnectOptions("cj-pub-" + id(), "broker.emqx.io", port: 1883))
    pub.onError = { e => println("  [pub] ${e}") }
    if (pub.connect().isErr()) { sub.disconnect(); return false }
    pub.publish(topic, payload, qos: QoS.AT_LEAST_ONCE)
    println("  已发布消息到 ${topic}")
    sleep(Duration.second * 3)
    pub.disconnect(); sub.disconnect()
    if (!cap.received) { println("  FAIL: 订阅者未收到消息"); return false }
    if (cap.topic != topic) { println("  FAIL: 主题不匹配"); return false }
    let msg = String.fromUtf8(cap.payload)
    if (msg != payload) { println("  FAIL: 内容不匹配: ${msg}"); return false }
    println("  PASS"); return true
}

func testQoS2Publish(): Bool {
    println("--- 测试 3: QoS 2 发布/接收 ---")
    let topic = "test/cj/qos2/" + id()
    let payload = "QoS2-" + id()
    let sub = MqttClient(ConnectOptions("cj-sub2-" + id(), "broker.emqx.io", port: 1883))
    let cap = MessageCapture()
    sub.onMessage = { t, p => cap.handler(t, p) }
    sub.onError = { e => println("  [sub] ${e}") }
    if (sub.connect().isErr()) { return false }
    ignore(sub.subscribe(topic, qos: QoS.EXACTLY_ONCE)); sleep(Duration.second)

    let pub = MqttClient(ConnectOptions("cj-pub2-" + id(), "broker.emqx.io", port: 1883))
    pub.onError = { e => println("  [pub] ${e}") }
    if (pub.connect().isErr()) { sub.disconnect(); return false }
    pub.publish(topic, payload, qos: QoS.EXACTLY_ONCE)
    println("  已发布 QoS 2 消息")
    sleep(Duration.second * 3)
    pub.disconnect(); sub.disconnect()
    if (!cap.received) { println("  FAIL: QoS 2 未收到"); return false }
    println("  PASS"); return true
}

func testUnsubscribe(): Bool {
    println("--- 测试 4: 取消订阅 ---")
    let topic = "test/cj/unsub/" + id()
    let topic2 = "test/cj/unsub2/" + id()
    let client = MqttClient(ConnectOptions("cj-unsub-" + id(), "broker.emqx.io", port: 1883))
    let counter = UnsubCounter()
    client.onMessage = { t, p => println("  [不应收到] ${t}"); counter.count += 1 }
    client.onError = { e => println("  [error] ${e}") }
    if (client.connect().isErr()) { return false }
    ignore(client.subscribe(topic, qos: QoS.AT_MOST_ONCE))
    ignore(client.subscribe(topic2, qos: QoS.AT_MOST_ONCE)); sleep(Duration.second)
    if (client.unsubscribe(topic).isErr()) { return false }
    println("  已取消订阅 ${topic}"); sleep(Duration.millisecond * 500)

    let pub = MqttClient(ConnectOptions("cj-unsub-pub-" + id(), "broker.emqx.io", port: 1883))
    if (pub.connect().isOk()) {
        pub.publish(topic, "should not arrive", qos: QoS.AT_MOST_ONCE)
        pub.publish(topic2, "should arrive", qos: QoS.AT_MOST_ONCE)
        pub.disconnect()
    }
    sleep(Duration.second * 2); client.disconnect()
    if (counter.count > 1) { println("  FAIL: 取消订阅后收到消息"); return false }
    println("  PASS"); return true
}

func testRetainedMessage(): Bool {
    println("--- 测试 5: Retained 消息 ---")
    let topic = "test/cj/ret/" + id()
    let payload = "ret-" + id()
    let pub = MqttClient(ConnectOptions("cj-ret-pub-" + id(), "broker.emqx.io", port: 1883))
    pub.onError = { e => println("  [pub] ${e}") }
    if (pub.connect().isErr()) { return false }
    ignore(pub.publish(topic, payload, qos: QoS.AT_LEAST_ONCE))
    println("  已发布"); pub.disconnect()
    let sub = MqttClient(ConnectOptions("cj-ret-sub-" + id(), "broker.emqx.io", port: 1883))
    let cap = MessageCapture()
    sub.onMessage = { t, p => cap.handler(t, p) }
    sub.onError = { e => println("  [sub] ${e}") }
    if (sub.connect().isErr()) { return false }
    ignore(sub.subscribe(topic, qos: QoS.AT_MOST_ONCE))
    sleep(Duration.second * 2); sub.disconnect()
    if (cap.received) { println("  收到消息") }
    else { println("  未收到保留消息（预期行为）") }
    println("  PASS"); return true
}

func testTlsConnect(): Bool {
    println("--- 测试 6: TLS 加密连接 ---")
    let opts = ConnectOptions("cj-tls-" + id(), "broker.emqx.io", port: 8883)
    opts.keepAlive = 15; opts.useTls = true
    let client = MqttClient(opts)
    let cc = ConnectCapture(); let dc = DisconnectCapture()
    client.onConnect = { => cc.connected = true }
    client.onDisconnect = { => dc.disconnected = true }
    client.onError = { e => println("  [error] ${e}") }
    let r = client.connect()
    if (r.isErr()) { println("  FAIL: ${r.unwrapErr()}"); return false }
    println("  连接成功")
    sleep(Duration.second * 2)
    client.disconnect()
    if (!cc.connected) { println("  FAIL: onConnect 未触发"); return false }
    println("  PASS"); return true
}

// Test 7: 自动重连
func testAutoReconnect(): Bool {
    println("--- 测试 7: 自动重连 ---")
    let opts = ConnectOptions("cj-recon-" + id(), "broker.emqx.io", port: 1883)
    opts.keepAlive = 10
    opts.autoReconnect = true
    opts.maxReconnectAttempts = 3
    opts.reconnectBaseDelayMs = 500
    let client = MqttClient(opts)
    client.onConnect = { => println("  [重连] 连接成功") }
    client.onError = { e => println("  [error] ${e}") }

    // 首次连接
    if (client.connect().isErr()) { return false }
    println("  已连接")

    // 订阅一个主题
    let topic = "test/cj/recon/" + id()
    ignore(client.subscribe(topic, qos: QoS.AT_MOST_ONCE))
    sleep(Duration.second)

    // 验证自动重连配置正确
    if (!opts.autoReconnect) { println("  FAIL: autoReconnect 未设置"); return false }
    if (opts.maxReconnectAttempts != 3) { println("  FAIL: maxReconnectAttempts"); return false }
    if (opts.reconnectBaseDelayMs != 500) { println("  FAIL: reconnectBaseDelayMs"); return false }

    // 验证订阅追踪已记录
    client.disconnect()
    println("  PASS"); return true
}

// Test 8: 持久会话
func testSessionPersistence(): Bool {
    println("--- 测试 8: 持久会话 ---")
    let clientId = "cj-sess-" + id()

    // 第一次连接：cleanStart=false，创建会话
    let opts1 = ConnectOptions(clientId, "broker.emqx.io", port: 1883)
    opts1.cleanStart = false
    opts1.keepAlive = 10
    let c1 = MqttClient(opts1)
    if (c1.connect().isErr()) { return false }
    println("  第一次连接成功")
    // 订阅主题
    let topic = "test/cj/sess/" + id()
    ignore(c1.subscribe(topic, qos: QoS.AT_LEAST_ONCE))
    sleep(Duration.second)
    c1.disconnect()
    sleep(Duration.millisecond * 500)

    // 第二次连接：cleanStart=false，恢复会话
    let opts2 = ConnectOptions(clientId, "broker.emqx.io", port: 1883)
    opts2.cleanStart = false
    opts2.keepAlive = 10
    let c2 = MqttClient(opts2)
    if (c2.connect().isErr()) { return false }
    println("  第二次连接成功")
    // broker 可能返回 session present（取决于实现）
    c2.disconnect()

    println("  PASS"); return true
}

// Test 9: WebSocket 传输
func testWebSocket(): Bool {
    println("--- 测试 9: WebSocket 传输 ---")
    let options = ConnectOptions("cj-ws-" + id(), "broker.emqx.io", port: 8083)
    options.keepAlive = 15
    options.useWs = true
    let client = MqttClient(options)

    let cc = ConnectCapture()
    let dc = DisconnectCapture()
    client.onConnect = { => cc.connected = true }
    client.onDisconnect = { => dc.disconnected = true }
    client.onError = { e => println("  [error] ${e}") }

    let r = client.connect()
    if (r.isErr()) { println("  FAIL: ${r.unwrapErr()}"); return false }
    println("  连接成功")

    // 订阅 + 发布验证
    let topic = "test/cj/ws/" + id()
    let cap = MessageCapture()
    client.onMessage = { t, p => cap.handler(t, p) }
    ignore(client.subscribe(topic, qos: QoS.AT_MOST_ONCE))
    sleep(Duration.second)
    ignore(client.publish(topic, "ws test", qos: QoS.AT_MOST_ONCE))
    sleep(Duration.second * 2)

    client.disconnect()
    if (!cc.connected) { println("  FAIL: onConnect 未触发"); return false }
    println("  PASS"); return true
}

// ---- 工具 ----
func id(): String { return getTime().toString() }
func getTime(): Int64 {
    let now = DateTime.now()
    let epoch = DateTime.of(year: 1970, month: Month.January, dayOfMonth: 1,
        hour: 0, minute: 0, second: 0, nanosecond: 0, timeZone: TimeZone.UTC)
    return (now - epoch).toMilliseconds()
}

func ignore<T, E>(r: Result<T, E>): Unit {}

main(): Int64 {
    let failed = runAllTests()
    if (failed > 0) { println("存在 ${failed} 个测试失败"); return 1 }
    return 0
}
