// 协议编解码测试
package mqtt.protocol

// ============ MQTT v3.1.1 编码测试 ============

@Test
func testConnectV311Encoding() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let options = ConnectOptions("test-client", "localhost")
    options.cleanStart = true
    options.keepAlive = 60

    let packet = codec.encodeConnect(options)

    @Expect(packet[0], UInt8(0x10))  // CONNECT type
    @Expect(packet[1] > UInt8(0), true)  // remaining length > 0
    @Expect(packet[4], UInt8(77))  // 'M'
    @Expect(packet[5], UInt8(81))  // 'Q'
    @Expect(packet[6], UInt8(84))  // 'T'
    @Expect(packet[7], UInt8(84))  // 'T'
    @Expect(packet[8], UInt8(4))   // protocol level v3.1.1
}

@Test
func testConnectV50Encoding() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    let options = ConnectOptions("test-client", "localhost")
    options.protocolVersion = ProtocolVersion.V5_0
    options.cleanStart = true
    options.keepAlive = 30

    let packet = codec.encodeConnect(options)

    @Expect(packet[0], UInt8(0x10))  // CONNECT type
    @Expect(packet[8], UInt8(5))     // protocol level v5.0
}

@Test
func testConnectWithCredentials() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let pw = Array<UInt8>(3, repeat: UInt8(0))
    pw[0] = UInt8(112); pw[1] = UInt8(119); pw[2] = UInt8(100)  // "pwd"
    let options = ConnectOptions("client", "broker.emqx.io")
    options.username = Some("user")
    options.password = Some(pw)
    options.cleanStart = true

    let packet = codec.encodeConnect(options)

    // Verify flags
    @Expect((packet[9] & UInt8(0x80)) != UInt8(0), true)  // Username flag
    @Expect((packet[9] & UInt8(0x40)) != UInt8(0), true)  // Password flag
}

@Test
func testPublishQoS0Encoding() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let payload = Array<UInt8>(5, repeat: UInt8(0))
    payload[0] = UInt8(72); payload[1] = UInt8(101); payload[2] = UInt8(108)
    payload[3] = UInt8(108); payload[4] = UInt8(111)

    let packet = codec.encodePublish("test/topic", payload, QoS.AT_MOST_ONCE, 0, false, false, MqttProperties())

    @Expect((packet[0] & UInt8(0xF0)), UInt8(0x30))  // PUBLISH type
    @Expect(((packet[0] & UInt8(0x06)) >> 1), UInt8(0))  // QoS 0
    @Expect((packet[0] & UInt8(0x01)), UInt8(0))  // Retain = 0
}

@Test
func testPublishQoS1Encoding() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let payload = Array<UInt8>(2, repeat: UInt8(0))
    payload[0] = UInt8(72); payload[1] = UInt8(105)

    let packet = codec.encodePublish("test/topic", payload, QoS.AT_LEAST_ONCE, 42, false, false, MqttProperties())

    @Expect((packet[0] & UInt8(0xF0)), UInt8(0x30))
    @Expect(((packet[0] & UInt8(0x06)) >> 1), UInt8(1))  // QoS 1
    // Topic length (2 bytes) + Topic + Packet ID (2 bytes)
    @Expect(packet[1] > UInt8(0), true)
}

@Test
func testPublishWithDupFlag() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let payload = Array<UInt8>(1, repeat: UInt8(0))
    payload[0] = UInt8(65)
    let packet = codec.encodePublish("topic", payload, QoS.AT_LEAST_ONCE, 1, false, true, MqttProperties())
    @Expect((packet[0] & UInt8(0x08)) != UInt8(0), true)  // DUP = 1
}

@Test
func testPublishWithRetain() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let payload = Array<UInt8>(1, repeat: UInt8(0))
    payload[0] = UInt8(65)
    let packet = codec.encodePublish("topic", payload, QoS.AT_MOST_ONCE, 0, true, false, MqttProperties())
    @Expect((packet[0] & UInt8(0x01)) != UInt8(0), true)  // Retain = 1
}

@Test
func testPublishV50Encoding() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    let payload = Array<UInt8>(1, repeat: UInt8(0))
    payload[0] = UInt8(65)
    let packet = codec.encodePublish("topic", payload, QoS.AT_MOST_ONCE, 0, false, false, MqttProperties())
    @Expect((packet[0] & UInt8(0xF0)), UInt8(0x30))
    // v5.0 has Properties length byte after topic (QoS 0 so no packet ID)
    // Topic: 2 bytes len + 5 bytes "topic" = 7, then Properties length
}

// ============ 编解码 Roundtrip 测试 ============

@Test
func testPubAckRoundtrip() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let originalPacketId: Int = 123
    let encoded = codec.encodePubAck(originalPacketId)
    let decoded = MqttDecoder.decode(encoded)
    @Expect(decoded.isSome(), true)
    let mqttPacket = decoded.getOrThrow()
    @Expect(mqttPacket.packetType.toInt(), 4)  // PUBACK
    let result = codec.parsePubAck(mqttPacket)
    @Expect(result.packetId, 123)
    @Expect(result.success, true)
    @Expect(result.reasonCode.toInt(), 0)  // Success
    @Expect(result.properties.isEmpty(), true)
}

@Test
func testPubRecRoundtrip() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let originalPacketId: Int = 456
    let encoded = codec.encodePubRec(originalPacketId)
    let decoded = MqttDecoder.decode(encoded)
    @Expect(decoded.isSome(), true)
    let mqttPacket = decoded.getOrThrow()
    @Expect(mqttPacket.packetType.toInt(), 5)  // PUBREC
    let result = codec.parsePubRec(mqttPacket)
    @Expect(result.packetId, 456)
    @Expect(result.success, true)
    @Expect(result.reasonCode.toInt(), 0)
}

@Test
func testPubRelRoundtrip() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let originalPacketId: Int = 789
    let encoded = codec.encodePubRel(originalPacketId)
    let decoded = MqttDecoder.decode(encoded)
    @Expect(decoded.isSome(), true)
    let mqttPacket = decoded.getOrThrow()
    @Expect(mqttPacket.packetType.toInt(), 6)  // PUBREL
    let result = codec.parsePubRel(mqttPacket)
    @Expect(result.packetId, 789)
    @Expect(result.success, true)
    @Expect(result.reasonCode.toInt(), 0)
}

@Test
func testPubCompRoundtrip() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let originalPacketId: Int = 999
    let encoded = codec.encodePubComp(originalPacketId)
    let decoded = MqttDecoder.decode(encoded)
    @Expect(decoded.isSome(), true)
    let mqttPacket = decoded.getOrThrow()
    @Expect(mqttPacket.packetType.toInt(), 7)  // PUBCOMP
    let result = codec.parsePubComp(mqttPacket)
    @Expect(result.packetId, 999)
    @Expect(result.success, true)
    @Expect(result.reasonCode.toInt(), 0)
}

@Test
func testSubscribeEncoding() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let subs = [Subscription("test/+", qos: QoS.AT_LEAST_ONCE)]
    let packet = codec.encodeSubscribe(subs, 1)

    @Expect(packet[0], UInt8(0x82))  // SUBSCRIBE type
    // Variable header: packet ID = 1 (2 bytes)
    @Expect(packet[1] > UInt8(0), true)
}

@Test
func testSubscribeV50Encoding() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    let subs = [Subscription("test/#", qos: QoS.EXACTLY_ONCE)]
    let packet = codec.encodeSubscribe(subs, 5)
    @Expect(packet[0], UInt8(0x82))
}

@Test
func testUnsubscribeEncoding() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let topics = ["test/topic", "test/other"]
    let packet = codec.encodeUnsubscribe(topics, 1)

    @Expect(packet[0], UInt8(0xA2))  // UNSUBSCRIBE type
    @Expect(packet[1] > UInt8(0), true)
}

@Test
func testPingReqEncoding() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let packet = codec.encodePingReq()
    @Expect(packet.size, Int64(2))
    @Expect(packet[0], UInt8(0xC0))
    @Expect(packet[1], UInt8(0x00))
}

@Test
func testDisconnectV311Encoding() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let packet = codec.encodeDisconnect(ReasonCode.Success, MqttProperties())
    @Expect(packet[0], UInt8(0xE0))
    @Expect(packet[1], UInt8(0x00))
}

@Test
func testDisconnectV50Encoding() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    let packet = codec.encodeDisconnect(ReasonCode.Success, MqttProperties())
    @Expect(packet[0], UInt8(0xE0))
    @Expect(packet[1], UInt8(0x02))
    @Expect(packet[2], UInt8(0x00))  // Reason code
    @Expect(packet[3], UInt8(0x00))  // Properties length
}

// ============ MQTT v3.1.1 解码测试 ============

@Test
func testConnAckAcceptedDecoding() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    // Build a CONNACK packet: 0x20 0x02 0x00 0x00 (session present=0, accepted)
    let data = Array<UInt8>(4, repeat: UInt8(0))
    data[0] = UInt8(0x20); data[1] = UInt8(0x02)
    data[2] = UInt8(0x00); data[3] = UInt8(0x00)

    let decoded = MqttDecoder.decode(data)
    @Expect(decoded.isSome(), true)
    let packet = decoded.getOrThrow()

    let result = codec.parseConnAck(packet)
    @Expect(result.sessionPresent, false)
    @Expect(result.returnCode.toInt(), 0)
}

@Test
func testConnAckWithSessionPresentDecoding() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let data = Array<UInt8>(4, repeat: UInt8(0))
    data[0] = UInt8(0x20); data[1] = UInt8(0x02)
    data[2] = UInt8(0x01); data[3] = UInt8(0x00)

    let decoded = MqttDecoder.decode(data)
    let packet = decoded.getOrThrow()
    let result = codec.parseConnAck(packet)
    @Expect(result.sessionPresent, true)
    @Expect(result.returnCode.toInt(), 0)
}

@Test
func testConnAckRejectedDecoding() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let data = Array<UInt8>(4, repeat: UInt8(0))
    data[0] = UInt8(0x20); data[1] = UInt8(0x02)
    data[2] = UInt8(0x00); data[3] = UInt8(0x05)  // not authorized

    let decoded = MqttDecoder.decode(data)
    let packet = decoded.getOrThrow()
    let result = codec.parseConnAck(packet)
    @Expect(result.sessionPresent, false)
    @Expect(result.returnCode.toInt(), 5)
}

@Test
func testPublishParseRoundtrip() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let payload = Array<UInt8>(5, repeat: UInt8(0))
    payload[0] = UInt8(72); payload[1] = UInt8(101); payload[2] = UInt8(108)
    payload[3] = UInt8(108); payload[4] = UInt8(111)

    let topic = "test/roundtrip"
    let packetBytes = codec.encodePublish(topic, payload, QoS.AT_LEAST_ONCE, 77, false, false, MqttProperties())

    let decoded = MqttDecoder.decode(packetBytes)
    @Expect(decoded.isSome(), true)
    let mqttPacket = decoded.getOrThrow()

    let result = codec.parsePublish(mqttPacket)

    @Expect(result.topic, topic)
    @Expect(result.qos.toInt(), 1)
    @Expect(result.retained, false)
    @Expect(result.packetId, 77)
    @Expect(result.payload.size, Int64(5))
}

@Test
func testPublishQoS0Parse() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let payload = Array<UInt8>(3, repeat: UInt8(0))
    payload[0] = UInt8(65); payload[1] = UInt8(66); payload[2] = UInt8(67)

    let packetBytes = codec.encodePublish("simple", payload, QoS.AT_MOST_ONCE, 0, false, false, MqttProperties())
    let decoded = MqttDecoder.decode(packetBytes)
    let mqttPacket = decoded.getOrThrow()
    let result = codec.parsePublish(mqttPacket)

    @Expect(result.topic, "simple")
    @Expect(result.qos.toInt(), 0)
    @Expect(result.payload.size, Int64(3))
}

// ============ MQTT v5.0 专有测试 ============

@Test
func testConnAckV5Decode() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    // Build CONNACK with sessionExpiryInterval=3600
    let data = Array<UInt8>(10, repeat: UInt8(0))
    data[0] = UInt8(0x20); data[1] = UInt8(0x08)
    data[2] = UInt8(0x00); data[3] = UInt8(0x00)
    data[4] = UInt8(0x05); data[5] = UInt8(0x11)
    data[6] = UInt8(0x00); data[7] = UInt8(0x00)
    data[8] = UInt8(0x0E); data[9] = UInt8(0x10)

    let decoded = MqttDecoder.decode(data)
    @Expect(decoded.isSome(), true)
    let packet = decoded.getOrThrow()

    let result = codec.parseConnAck(packet)
    @Expect(result.sessionPresent, false)
    @Expect(result.returnCode.toInt(), Int64(0))
    @Expect(result.properties.isEmpty(), false)
    @Expect(result.properties.sessionExpiryInterval.getOrThrow(), Int64(3600))
}

@Test
func testConnAckV5NoProperties() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    // CONNACK without properties (same as v3.1.1 format)
    let data = Array<UInt8>(4, repeat: UInt8(0))
    data[0] = UInt8(0x20); data[1] = UInt8(0x02)
    data[2] = UInt8(0x00); data[3] = UInt8(0x00)

    let decoded = MqttDecoder.decode(data)
    let packet = decoded.getOrThrow()

    let result = codec.parseConnAck(packet)
    @Expect(result.properties.isEmpty(), true)
}

@Test
func testPublishV50WithProperties() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    let payload = Array<UInt8>(3, repeat: UInt8(0))
    payload[0] = UInt8(65); payload[1] = UInt8(66); payload[2] = UInt8(67)

    let props = MqttProperties()
    props.payloadFormatIndicator = Some(true)

    let packetBytes = codec.encodePublish("test/v5", payload, QoS.AT_MOST_ONCE, 0, false, false, props)

    let decoded = MqttDecoder.decode(packetBytes)
    @Expect(decoded.isSome(), true)
    let mqttPacket = decoded.getOrThrow()

    let result = codec.parsePublish(mqttPacket)

    @Expect(result.topic, "test/v5")
    @Expect(result.qos.toInt(), 0)
    @Expect(result.retained, false)
    @Expect(result.packetId, 0)
    @Expect(result.properties.isEmpty(), false)
    @Expect(result.properties.payloadFormatIndicator.getOrThrow(), true)
}

@Test
func testPubAckV5Success() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    // Encode v3.1.1 PUBACK (no reason code) and decode as v5
    let encoded = codec.encodePubAck(42)
    let decoded = MqttDecoder.decode(encoded)
    @Expect(decoded.isSome(), true)
    let mqttPacket = decoded.getOrThrow()

    let result = codec.parsePubAck(mqttPacket)
    @Expect(result.packetId, 42)
    @Expect(result.reasonCode.toInt(), 0)  // Success
    @Expect(result.properties.isEmpty(), true)
}

@Test
func testPubAckV5WithReasonCode() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    // Build PUBACK with reasonCode=131 (ImplementationSpecificError)
    // 0x40 0x04 0x00 0x2A 0x83 0x00
    let data = Array<UInt8>(6, repeat: UInt8(0))
    data[0] = UInt8(0x40); data[1] = UInt8(0x04)
    data[2] = UInt8(0x00); data[3] = UInt8(0x2A)
    data[4] = UInt8(0x83); data[5] = UInt8(0x00)

    let decoded = MqttDecoder.decode(data)
    @Expect(decoded.isSome(), true)
    let mqttPacket = decoded.getOrThrow()

    let result = codec.parsePubAck(mqttPacket)
    @Expect(result.packetId, 42)
    @Expect(result.reasonCode.toInt(), 131)  // ImplementationSpecificError
    @Expect(result.success, false)  // reasonCode >= 0x80
    @Expect(result.properties.isEmpty(), true)
}

@Test
func testPubRecV5Roundtrip() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    // PUBREC: packetId=100, reasonCode=Success, empty properties
    let data = Array<UInt8>(6, repeat: UInt8(0))
    data[0] = UInt8(0x50); data[1] = UInt8(0x04)
    data[2] = UInt8(0x00); data[3] = UInt8(0x64)
    data[4] = UInt8(0x00); data[5] = UInt8(0x00)

    let decoded = MqttDecoder.decode(data)
    @Expect(decoded.isSome(), true)
    let mqttPacket = decoded.getOrThrow()

    let result = codec.parsePubRec(mqttPacket)
    @Expect(result.packetId, 100)
}

@Test
func testPubRelV5Roundtrip() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    // PUBREL: packetId=200, reasonCode=Success, empty properties
    let data = Array<UInt8>(6, repeat: UInt8(0))
    data[0] = UInt8(0x62); data[1] = UInt8(0x04)
    data[2] = UInt8(0x00); data[3] = UInt8(0xC8)
    data[4] = UInt8(0x00); data[5] = UInt8(0x00)

    let decoded = MqttDecoder.decode(data)
    @Expect(decoded.isSome(), true)
    let mqttPacket = decoded.getOrThrow()

    let result = codec.parsePubRel(mqttPacket)
    @Expect(result.packetId, 200)
}

@Test
func testPubCompV5Roundtrip() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    // PUBCOMP: packetId=300, reasonCode=Success, empty properties
    let data = Array<UInt8>(6, repeat: UInt8(0))
    data[0] = UInt8(0x70); data[1] = UInt8(0x04)
    data[2] = UInt8(0x01); data[3] = UInt8(0x2C)
    data[4] = UInt8(0x00); data[5] = UInt8(0x00)

    let decoded = MqttDecoder.decode(data)
    @Expect(decoded.isSome(), true)
    let mqttPacket = decoded.getOrThrow()

    let result = codec.parsePubComp(mqttPacket)
    @Expect(result.packetId, 300)
}

@Test
func testSubAckV5Decode() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    // SUBACK: packetId=1, empty properties, 3 reason codes
    // 0x90 0x06 0x00 0x01 0x00 0x00 0x80 0x84
    let data = Array<UInt8>(8, repeat: UInt8(0))
    data[0] = UInt8(0x90); data[1] = UInt8(0x06)
    data[2] = UInt8(0x00); data[3] = UInt8(0x01)
    data[4] = UInt8(0x00); data[5] = UInt8(0x00)
    data[6] = UInt8(0x80); data[7] = UInt8(0x84)

    let decoded = MqttDecoder.decode(data)
    @Expect(decoded.isSome(), true)
    let mqttPacket = decoded.getOrThrow()

    let result = codec.parseSubAck(mqttPacket)
    @Expect(result.packetId, Int64(1))
    @Expect(result.grantedQos.size, Int64(3))
    // 0x00 -> QoS 0 granted(-1=0), 0x80 -> failure(-1), 0x84 -> failure(-1)
    @Expect(result.grantedQos[0], 0)
    @Expect(result.grantedQos[1], -1)
    @Expect(result.grantedQos[2], -1)
}

@Test
func testUnsubAckV5Decode() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    // UNSUBACK: packetId=5, empty properties, 2 reason codes
    // 0xB0 0x05 0x00 0x05 0x00 0x00 0x11
    let data = Array<UInt8>(7, repeat: UInt8(0))
    data[0] = UInt8(0xB0); data[1] = UInt8(0x05)
    data[2] = UInt8(0x00); data[3] = UInt8(0x05)
    data[4] = UInt8(0x00); data[5] = UInt8(0x00)
    data[6] = UInt8(0x11)

    let decoded = MqttDecoder.decode(data)
    @Expect(decoded.isSome(), true)
    let mqttPacket = decoded.getOrThrow()

    let result = codec.parseUnsubAck(mqttPacket)
    @Expect(result.packetId, Int64(5))
    @Expect(result.success, true)
}

@Test
func testDisconnectV5Decode() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    // DISCONNECT: reasonCode=132 (UnsupportedProtocolVersion), empty properties
    // 0xE0 0x02 0x84 0x00
    let data = Array<UInt8>(4, repeat: UInt8(0))
    data[0] = UInt8(0xE0); data[1] = UInt8(0x02)
    data[2] = UInt8(0x84); data[3] = UInt8(0x00)

    let decoded = MqttDecoder.decode(data)
    @Expect(decoded.isSome(), true)
    let mqttPacket = decoded.getOrThrow()

    let result = codec.parseDisconnect(mqttPacket)
    @Expect(result.reasonCode.toInt(), 132)  // UnsupportedProtocolVersion
}

@Test
func testAuthEncodeDecode() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    let encoded = codec.encodeAuth(ReasonCode.ContinueAuthentication, MqttProperties())
    let decoded = MqttDecoder.decode(encoded)
    @Expect(decoded.isSome(), true)
    let mqttPacket = decoded.getOrThrow()

    let result = codec.parseAuth(mqttPacket)
    @Expect(result.reasonCode.toInt(), 24)  // ContinueAuthentication
    @Expect(result.properties.isEmpty(), true)
}

// ============ Properties 编解码测试 ============

@Test
func testPropertiesDecodeEmpty() {
    let empty = Array<UInt8>()
    let props = MqttProperties.decode(empty)
    @Expect(props.isEmpty(), true)
}

@Test
func testPropertiesDecodeMultiple() {
    // sessionExpiryInterval(0x11) + 4 bytes(3600) + receiveMaximum(0x21) + 2 bytes(100)
    let data = Array<UInt8>(8, repeat: UInt8(0))
    data[0] = UInt8(0x11)
    data[1] = UInt8(0x00); data[2] = UInt8(0x00)
    data[3] = UInt8(0x0E); data[4] = UInt8(0x10)  // 3600
    data[5] = UInt8(0x21)
    data[6] = UInt8(0x00); data[7] = UInt8(0x64)  // 100

    let props = MqttProperties.decode(data)
    @Expect(props.isEmpty(), false)
    @Expect(props.sessionExpiryInterval.isSome(), true)
    @Expect(props.sessionExpiryInterval.getOrThrow(), Int64(3600))
    @Expect(props.receiveMaximum.isSome(), true)
    @Expect(props.receiveMaximum.getOrThrow(), Int64(100))
}


// ============ 边界条件与错误处理测试 ============

@Test
func testDecodeEmptyData() {
    let data = Array<UInt8>()
    let result = MqttDecoder.decode(data)
    @Expect(result.isNone(), true)
}

@Test
func testDecodeIncompleteHeader() {
    let data = Array<UInt8>(1, repeat: UInt8(0))
    data[0] = UInt8(0x10)
    let result = MqttDecoder.decode(data)
    @Expect(result.isNone(), true)
}

@Test
func testCodecFactoryV311() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let packet = codec.encodeDisconnect(ReasonCode.Success, MqttProperties())
    @Expect(packet.size, Int64(2))
    @Expect(packet[0], UInt8(0xE0))
    @Expect(packet[1], UInt8(0x00))
}

@Test
func testCodecFactoryV50() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    let packet = codec.encodeDisconnect(ReasonCode.Success, MqttProperties())
    @Expect(packet.size, Int64(4))
    @Expect(packet[2], UInt8(0x00))
    @Expect(packet[3], UInt8(0x00))
}

// ============ v3.1.1 解码归一化测试 ============

@Test
func testSubAckV311Normalization() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let data = Array<UInt8>(8, repeat: UInt8(0))
    data[0] = UInt8(0x90); data[1] = UInt8(0x06)
    data[2] = UInt8(0x00); data[3] = UInt8(0x01)
    data[4] = UInt8(0x00); data[5] = UInt8(0x01)
    data[6] = UInt8(0x02); data[7] = UInt8(0x80)

    let decoded = MqttDecoder.decode(data)
    let mqttPacket = decoded.getOrThrow()
    let result = codec.parseSubAck(mqttPacket)
    @Expect(result.packetId, Int64(1))
    @Expect(result.grantedQos.size, Int64(4))
    @Expect(result.grantedQos[0], 0)
    @Expect(result.grantedQos[1], 1)
    @Expect(result.grantedQos[2], 2)
    @Expect(result.grantedQos[3], -1)
}

@Test
func testUnsubAckV311Decode() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let data = Array<UInt8>(4, repeat: UInt8(0))
    data[0] = UInt8(0xB0); data[1] = UInt8(0x02)
    data[2] = UInt8(0x00); data[3] = UInt8(0x07)

    let decoded = MqttDecoder.decode(data)
    let packet = decoded.getOrThrow()
    let result = codec.parseUnsubAck(packet)
    @Expect(result.packetId, Int64(7))
    @Expect(result.success, true)
}

@Test
func testConnAckAllReturnCodes() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let codes = [0, 1, 2, 3, 4, 5]
    for (code in codes) {
        let data = Array<UInt8>(4, repeat: UInt8(0))
        data[0] = UInt8(0x20); data[1] = UInt8(0x02)
        data[2] = UInt8(0x00); data[3] = UInt8(Int64(code))

        let decoded = MqttDecoder.decode(data)
        let packet = decoded.getOrThrow()
        let result = codec.parseConnAck(packet)
        @Expect(result.returnCode.toInt(), Int64(code))
    }
}

@Test
func testPingRespDecoding() {
    let data = Array<UInt8>(2, repeat: UInt8(0))
    data[0] = UInt8(0xD0); data[1] = UInt8(0x00)

    let decoded = MqttDecoder.decode(data)
    let packet = decoded.getOrThrow()
    @Expect(packet.packetType.toInt(), 13)
}

// ============ v5.0 深入解码测试 ============

@Test
func testDisconnectV5WithReasonCode() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    let data = Array<UInt8>(4, repeat: UInt8(0))
    data[0] = UInt8(0xE0); data[1] = UInt8(0x02)
    data[2] = UInt8(0x8D); data[3] = UInt8(0x00)

    let decoded = MqttDecoder.decode(data)
    let mqttPacket = decoded.getOrThrow()
    let result = codec.parseDisconnect(mqttPacket)
    @Expect(result.reasonCode.toInt(), 141)
    @Expect(result.properties.isEmpty(), true)
}

@Test
func testDisconnectV5OnlyReasonCode() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    let data = Array<UInt8>(3, repeat: UInt8(0))
    data[0] = UInt8(0xE0); data[1] = UInt8(0x01)
    data[2] = UInt8(0x84)

    let decoded = MqttDecoder.decode(data)
    let mqttPacket = decoded.getOrThrow()
    let result = codec.parseDisconnect(mqttPacket)
    @Expect(result.reasonCode.toInt(), 132)
}

// ============ Roundtrip 一致性测试 ============

@Test
func testPublishQoS2Roundtrip() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let payload = Array<UInt8>(4, repeat: UInt8(0))
    payload[0] = UInt8(84); payload[1] = UInt8(101)
    payload[2] = UInt8(115); payload[3] = UInt8(116)

    let packetBytes = codec.encodePublish("qos2/test", payload, QoS.EXACTLY_ONCE, 42, false, false, MqttProperties())

    let decoded = MqttDecoder.decode(packetBytes)
    let mqttPacket = decoded.getOrThrow()
    let result = codec.parsePublish(mqttPacket)

    @Expect(result.topic, "qos2/test")
    @Expect(result.qos.toInt(), 2)
    @Expect(result.packetId, 42)
    @Expect(result.payload.size, Int64(4))
}

@Test
func testAllQoSAckRoundtrip() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)

    let ackEnc = codec.encodePubAck(100)
    let ackDec = MqttDecoder.decode(ackEnc).getOrThrow()
    let ackResult = codec.parsePubAck(ackDec)
    @Expect(ackResult.packetId, 100)
    @Expect(ackResult.success, true)
    @Expect(ackResult.reasonCode.toInt(), 0)

    let recEnc = codec.encodePubRec(200)
    let recDec = MqttDecoder.decode(recEnc).getOrThrow()
    let recResult = codec.parsePubRec(recDec)
    @Expect(recResult.packetId, 200)
    @Expect(recResult.success, true)

    let relEnc = codec.encodePubRel(300)
    let relDec = MqttDecoder.decode(relEnc).getOrThrow()
    let relResult = codec.parsePubRel(relDec)
    @Expect(relResult.packetId, 300)
    @Expect(relResult.success, true)

    let compEnc = codec.encodePubComp(400)
    let compDec = MqttDecoder.decode(compEnc).getOrThrow()
    let compResult = codec.parsePubComp(compDec)
    @Expect(compResult.packetId, 400)
    @Expect(compResult.success, true)
}

@Test
func testV50PubAckWithReasonCode() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    let data = Array<UInt8>(6, repeat: UInt8(0))
    data[0] = UInt8(0x40); data[1] = UInt8(0x04)
    data[2] = UInt8(0x00); data[3] = UInt8(0x2A)
    data[4] = UInt8(0x83); data[5] = UInt8(0x00)

    let decoded = MqttDecoder.decode(data)
    let mqttPacket = decoded.getOrThrow()
    let result = codec.parsePubAck(mqttPacket)
    @Expect(result.packetId, 42)
    @Expect(result.reasonCode.toInt(), 131)
    @Expect(result.success, false)
}

@Test
func testV50ConnAckWithServerKeepAlive() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    let data = Array<UInt8>(8, repeat: UInt8(0))
    data[0] = UInt8(0x20); data[1] = UInt8(0x06)
    data[2] = UInt8(0x00); data[3] = UInt8(0x00)
    data[4] = UInt8(0x03)
    data[5] = UInt8(0x13)
    data[6] = UInt8(0x00); data[7] = UInt8(0x1E)

    let decoded = MqttDecoder.decode(data)
    let packet = decoded.getOrThrow()
    let result = codec.parseConnAck(packet)
    @Expect(result.sessionPresent, false)
    @Expect(result.returnCode.toInt(), 0)
    @Expect(result.properties.serverKeepAlive.isSome(), true)
    @Expect(result.properties.serverKeepAlive.getOrThrow(), Int64(30))
}

// ============ V311 vs V50 编码差异验证 ============

@Test
func testV311EncodeSubscribeNoProperties() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let subs = [Subscription("test/#", qos: QoS.AT_LEAST_ONCE)]
    let packet = codec.encodeSubscribe(subs, 1)

    @Expect(packet[0], UInt8(0x82))
    @Expect(packet[1], UInt8(11))
}

@Test
func testV311EncodeUnsubscribeNoProperties() {
    let codec = ProtocolCodec.create(ProtocolVersion.V3_1_1)
    let topics = ["test/topic"]
    let packet = codec.encodeUnsubscribe(topics, 1)

    @Expect(packet[0], UInt8(0xA2))
    @Expect(packet[1], UInt8(14))
}

@Test
func testV50EncodeSubscribeHasProperties() {
    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    let subs = [Subscription("test/#", qos: QoS.AT_LEAST_ONCE)]
    let packet = codec.encodeSubscribe(subs, 1)

    @Expect(packet[0], UInt8(0x82))
    @Expect(packet[1], UInt8(12))
}
