// v5.0 Properties 编码测试
package mqtt.protocol

@Test
func testPropertiesEmpty() {
    let props = MqttProperties()
    let data = props.encode()
    @Expect(data.size, Int64(0))
}

@Test
func testPropertiesSessionExpiry() {
    let props = MqttProperties()
    props.sessionExpiryInterval = Some(3600)
    let data = props.encode()

    // Should have: 1 byte ID + 4 bytes value = 5 bytes
    @Expect(data[0], UInt8(0x11))  // Property ID
    // 3600 = 0x00000E10
    @Expect(data[1], UInt8(0x00))
    @Expect(data[2], UInt8(0x00))
    @Expect(data[3], UInt8(0x0E))
    @Expect(data[4], UInt8(0x10))
}

@Test
func testPropertiesReceiveMaximum() {
    let props = MqttProperties()
    props.receiveMaximum = Some(100)
    let data = props.encode()

    @Expect(data[0], UInt8(0x21))  // Property ID
    // 100 = 0x0064
    @Expect(data[1], UInt8(0x00))
    @Expect(data[2], UInt8(0x64))
}

@Test
func testPropertiesPayloadFormat() {
    let props = MqttProperties()
    props.payloadFormatIndicator = Some(true)
    let data = props.encode()

    @Expect(data[0], UInt8(0x01))  // Property ID
    @Expect(data[1], UInt8(0x01))  // true = 1
}

@Test
func testPropertiesUserProperty() {
    let props = MqttProperties()
    props.userProperties.add("key1", "val1")
    let data = props.encode()

    @Expect(data[0], UInt8(0x26))  // User Property ID
    // String length (2 bytes) + "key1" (4 bytes)
    @Expect(data[1], UInt8(0x00))
    @Expect(data[2], UInt8(0x04))
    @Expect(data[3], UInt8(107))  // 'k'
    @Expect(data[4], UInt8(101))  // 'e'
    @Expect(data[5], UInt8(121))  // 'y'
    @Expect(data[6], UInt8(49))   // '1'
    // "val1" length
    @Expect(data[7], UInt8(0x00))
    @Expect(data[8], UInt8(0x04))
    @Expect(data[9], UInt8(118))  // 'v'
}

@Test
func testPropertiesMultiple() {
    let props = MqttProperties()
    props.sessionExpiryInterval = Some(86400)
    props.receiveMaximum = Some(50)
    props.payloadFormatIndicator = Some(true)
    let data = props.encode()

    // 3 properties should be encoded in order
    @Expect(data[0], UInt8(0x11))  // Session Expiry (first)
    @Expect(data[5], UInt8(0x21))  // Receive Maximum (second)
    @Expect(data[8], UInt8(0x01))  // Payload Format (third)
}

@Test
func testConnectOptionsV5Properties() {
    let opts = ConnectOptions("v5-test", "broker.emqx.io")
    opts.protocolVersion = ProtocolVersion.V5_0
    opts.properties.sessionExpiryInterval = Some(3600)

    let codec = ProtocolCodec.create(ProtocolVersion.V5_0)
    let packet = codec.encodeConnect(opts)
    @Expect(packet[8], UInt8(5))  // Protocol level = 5
    // Verify properties data appears somewhere in the packet
    var foundSessionExpiry: Bool = false
    for (i in 0..packet.size) {
        if (packet[Int64(i)] == UInt8(0x11)) { foundSessionExpiry = true; break }
    }
    @Expect(foundSessionExpiry, true)
}
