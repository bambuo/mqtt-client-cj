// 基础功能测试
package mqtt

import mqtt.protocol.*
import mqtt.store.*

@Test
func testPacketTypeEnum() {
    @Expect(PacketType.CONNECT.toInt(), 1)
    @Expect(PacketType.CONNACK.toInt(), 2)
    @Expect(PacketType.PUBLISH.toInt(), 3)
    @Expect(PacketType.PUBACK.toInt(), 4)
    @Expect(PacketType.PUBREC.toInt(), 5)
    @Expect(PacketType.PUBREL.toInt(), 6)
    @Expect(PacketType.PUBCOMP.toInt(), 7)
    @Expect(PacketType.SUBSCRIBE.toInt(), 8)
    @Expect(PacketType.SUBACK.toInt(), 9)
    @Expect(PacketType.UNSUBSCRIBE.toInt(), 10)
    @Expect(PacketType.UNSUBACK.toInt(), 11)
    @Expect(PacketType.PINGREQ.toInt(), 12)
    @Expect(PacketType.PINGRESP.toInt(), 13)
    @Expect(PacketType.DISCONNECT.toInt(), 14)
    @Expect(PacketType.AUTH.toInt(), 15)
}

@Test
func testPacketTypeFromInt() {
    @Expect(PacketType.fromInt(1).toInt(), 1)
    @Expect(PacketType.fromInt(3).toInt(), 3)
    @Expect(PacketType.fromInt(14).toInt(), 14)
}

@Test
func testQoSEnum() {
    @Expect(QoS.AT_MOST_ONCE.toInt(), 0)
    @Expect(QoS.AT_LEAST_ONCE.toInt(), 1)
    @Expect(QoS.EXACTLY_ONCE.toInt(), 2)
}

@Test
func testQoSFromInt() {
    @Expect(QoS.fromInt(0).toInt(), 0)
    @Expect(QoS.fromInt(1).toInt(), 1)
    @Expect(QoS.fromInt(2).toInt(), 2)
}

@Test
func testProtocolVersion() {
    @Expect(ProtocolVersion.V3_1_1.toInt(), 4)
    @Expect(ProtocolVersion.V5_0.toInt(), 5)
}

@Test
func testProtocolVersionFromInt() {
    @Expect(ProtocolVersion.fromInt(4).toInt(), 4)
    @Expect(ProtocolVersion.fromInt(5).toInt(), 5)
}

@Test
func testConnectReturnCodeFromInt() {
    @Expect(ConnectReturnCode.fromInt(0).toInt(), 0)
    @Expect(ConnectReturnCode.fromInt(1).toInt(), 1)
    @Expect(ConnectReturnCode.fromInt(2).toInt(), 2)
    @Expect(ConnectReturnCode.fromInt(3).toInt(), 3)
    @Expect(ConnectReturnCode.fromInt(4).toInt(), 4)
    @Expect(ConnectReturnCode.fromInt(5).toInt(), 5)
}

@Test
func testConnectOptions() {
    let options = ConnectOptions("test-client", "localhost", port: 1883)

    @Expect(options.clientId, "test-client")
    @Expect(options.host, "localhost")
    @Expect(options.port, 1883)
    @Expect(options.cleanStart, true)
    @Expect(options.keepAlive, 60)
    @Expect(options.protocolVersion.toInt(), 4)
    @Expect(options.willTopic.isNone(), true)
    @Expect(options.username.isNone(), true)
    @Expect(options.password.isNone(), true)
}

@Test
func testConnectOptionsCustom() {
    let pw: Array<UInt8> = Array<UInt8>(3, repeat: UInt8(0))
    pw[0] = UInt8(112); pw[1] = UInt8(119); pw[2] = UInt8(100)
    let options = ConnectOptions("custom", "broker.emqx.io", port: 1883)
    options.cleanStart = false
    options.keepAlive = 120
    options.protocolVersion = ProtocolVersion.V5_0
    options.username = Some("user1")
    options.password = Some(pw)

    @Expect(options.clientId, "custom")
    @Expect(options.cleanStart, false)
    @Expect(options.keepAlive, 120)
    @Expect(options.protocolVersion.toInt(), 5)
    @Expect(options.username.getOrThrow(), "user1")
    @Expect(options.password.getOrThrow()[0], UInt8(112))
}

@Test
func testSubscription() {
    let sub = Subscription("test/topic", qos: QoS.AT_LEAST_ONCE)
    @Expect(sub.topic, "test/topic")
    @Expect(sub.qos.toInt(), 1)
}

@Test
func testMqttPacket() {
    let payload = Array<UInt8>(3, repeat: UInt8(0))
    payload[0] = UInt8(1); payload[1] = UInt8(2); payload[2] = UInt8(3)
    let packet = MqttPacket(PacketType.PUBLISH, UInt8(0x30), payload)
    @Expect(packet.packetType.toInt(), 3)
    @Expect(packet.flags, UInt8(0x30))
    @Expect(packet.payload.size, Int64(3))
}

@Test
func testMqttMessage() {
    let payload = Array<UInt8>(2, repeat: UInt8(0))
    payload[0] = UInt8(72); payload[1] = UInt8(105)
    let msg = MqttMessage("test", payload, QoS.AT_MOST_ONCE, false, packetId: None)
    @Expect(msg.topic, "test")
    @Expect(msg.qos.toInt(), 0)
    @Expect(msg.retained, false)
    @Expect(msg.packetId.isNone(), true)
}

@Test
func testResultType() {
    let ok: Result<Int64, String> = Result.Ok(42)
    @Expect(ok.isOk(), true)
    @Expect(ok.isErr(), false)
    @Expect(ok.unwrap(), 42)

    let err: Result<Int64, String> = Result.Err("error msg")
    @Expect(err.isOk(), false)
    @Expect(err.isErr(), true)
    @Expect(err.unwrapErr(), "error msg")
}

@Test
func testInflightState() {
    @Expect(InflightState.SENT.toInt(), 0)
    @Expect(InflightState.RECEIVED.toInt(), 1)
    @Expect(InflightState.WAIT_PUBREL.toInt(), 2)
}

@Test
func testInflightMessage() {
    let payload = Array<UInt8>(3, repeat: UInt8(0))
    payload[0] = UInt8(1); payload[1] = UInt8(2); payload[2] = UInt8(3)
    let msg = InflightMessage(Int64(1), payload, InflightState.SENT)
    @Expect(msg.packetId, Int64(1))
    @Expect(msg.state.toInt(), 0)
    @Expect(msg.retryCount, Int64(0))
}

@Test
func testInflightMessageUpdateTimestamp() {
    let payload = Array<UInt8>(3, repeat: UInt8(0))
    payload[0] = UInt8(1); payload[1] = UInt8(2); payload[2] = UInt8(3)
    let msg = InflightMessage(Int64(5), payload, InflightState.SENT)
    msg.updateTimestamp()
    @Expect(msg.retryCount, Int64(1))
    msg.updateTimestamp()
    @Expect(msg.retryCount, Int64(2))
}

@Test
func testMessageStore() {
    let store = MessageStore()
    @Expect(store.size(), Int64(0))

    let id1 = store.generatePacketId()
    @Expect(id1, Int64(1))
    let id2 = store.generatePacketId()
    @Expect(id2, Int64(2))

    let data = Array<UInt8>(3, repeat: UInt8(0))
    data[0] = UInt8(10); data[1] = UInt8(20); data[2] = UInt8(30)
    store.store(id1, data, InflightState.SENT)
    @Expect(store.size(), Int64(1))

    let retrieved = store.get(id1)
    @Expect(retrieved.isSome(), true)
    @Expect(retrieved.getOrThrow().packetId, id1)

    let removed = store.remove(id1)
    @Expect(removed.isSome(), true)
    @Expect(store.size(), Int64(0))
}
