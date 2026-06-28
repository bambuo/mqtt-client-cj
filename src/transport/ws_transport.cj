// WebSocket 传输层
package mqtt.transport

import stdx.net.http.*
import stdx.encoding.url.*
import std.collection.*
import mqtt.protocol.Result
import mqtt.protocol.Transport

public class WsTransport <: Transport {
    private var client: ?Client = None
    private var ws: ?WebSocket = None
    private var host: String
    private var port: Int
    private var wsPath: String
    private var useTls: Bool = false
    private var receiveCallback: ?(Array<Byte>) -> Unit = None
    private var isRunning: Bool = false

    public init(host: String, port: Int, useTls!: Bool = false, wsPath!: String = "/mqtt") {
        this.host = host
        this.port = port
        this.useTls = useTls
        this.wsPath = wsPath
    }

    // 连接到 WebSocket 服务器
    public func connect(): Result<Unit, String> {
        return try {
            let httpClient = ClientBuilder().build()
            client = Some(httpClient)

            // 构建 WS/WSS URL
            let scheme = if (useTls) { "wss" } else { "ws" }
            let urlStr = "${scheme}://${host}:${port}${wsPath}"
            let url = URL.parse(urlStr)

            // 设置 MQTT 子协议
            let subProtocols = ArrayList<String>()
            subProtocols.add("mqtt")

            let (wsSocket, _) = WebSocket.upgradeFromClient(httpClient, url,
                subProtocols: subProtocols)
            ws = Some(wsSocket)
            isRunning = true  // 在 WebSocket 升级成功后才标记为运行中

            // 启动接收线程
            startReceiveThread()

            Ok(())
        } catch (e: Exception) {
            Err("WebSocket 连接失败: " + e.message)
        }
    }

    // 发送数据（MQTT 包作为二进制帧）
    public func send(data: Array<Byte>): Result<Int, String> {
        return try {
            if (ws.isNone()) {
                return Err("WebSocket 未连接")
            }
            ws.getOrThrow().write(BinaryWebFrame, data)
            Ok(data.size)
        } catch (e: Exception) {
            Err("WebSocket 发送失败: " + e.message)
        }
    }

    // 设置接收回调
    public func onReceive(callback: (Array<Byte>) -> Unit) {
        this.receiveCallback = Some(callback)
    }

    // 接收线程
    private func startReceiveThread() {
        spawn {
            while (isRunning && ws.isSome()) {
                try {
                    let wsSocket = ws.getOrThrow()
                    let frame = wsSocket.read()

                    match (frame.frameType) {
                        case BinaryWebFrame =>
                            // 收到 MQTT 数据
                            let data = frame.payload
                            if (receiveCallback.isSome() && data.size > 0) {
                                receiveCallback.getOrThrow()(data)
                            }
                        case TextWebFrame =>
                            // MQTT over WS 使用二进制帧，文本帧忽略
                            ()
                        case PingWebFrame =>
                            // 自动回复 Pong
                            wsSocket.writePongFrame(frame.payload)
                        case PongWebFrame =>
                            ()
                        case CloseWebFrame =>
                            isRunning = false
                            break
                        case _ =>
                            ()
                    }

                } catch (e: Exception) {
                    if (isRunning) {
                        println("WebSocket 接收错误: " + e.message)
                    }
                    isRunning = false
                    break
                }
            }
            isRunning = false
        }
    }

    // 关闭连接
    public func close() {
        isRunning = false
        try {
            if (ws.isSome()) {
                let wsSocket = ws.getOrThrow()
                wsSocket.writeCloseFrame(status: UInt16(1000))
                let _ = wsSocket.read()
                wsSocket.closeConn()
                ws = None
            }
        } catch (e: Exception) {}
        try {
            if (client.isSome()) {
                client.getOrThrow().close()
                client = None
            }
        } catch (e: Exception) {}
    }

    // 检查连接状态
    public func isConnected(): Bool {
        return ws.isSome() && isRunning
    }
}