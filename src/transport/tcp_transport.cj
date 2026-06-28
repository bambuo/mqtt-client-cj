// TCP/TLS 传输层
package mqtt.transport

import std.net.*
import stdx.net.tls.*
import mqtt.protocol.Result
import mqtt.protocol.TlsOptions
import mqtt.protocol.Transport

public class TcpTransport <: Transport {
    private var socket: ?TcpSocket = None
    private var tlsSocket: ?TlsSocket = None
    private var host: String
    private var port: Int
    private var receiveCallback: ?(Array<Byte>) -> Unit = None
    private var isRunning: Bool = false
    private var useTls: Bool = false
    private var tlsConfig: TlsOptions = TlsOptions()

    public init(host: String, port: Int, useTls!: Bool = false, tlsConfig!: TlsOptions = TlsOptions()) {
        this.host = host
        this.port = port
        this.useTls = useTls
        this.tlsConfig = tlsConfig
    }

    // 连接到 broker
    public func connect(): Result<Unit, String> {
        return try {
            let sock = TcpSocket(host, UInt16(port))
            sock.connect()
            socket = Some(sock)

            // TLS 握手（如果启用）
            if (useTls) {
                let tlsClientConfig = TlsClientConfig()

                let tls = TlsSocket.client(sock, clientConfig: tlsClientConfig)
                tls.handshake()
                tlsSocket = Some(tls)
                println("TLS 握手成功")
            }

            startReceiveThread()
            Ok(())
        } catch (e: Exception) {
            Err("连接失败: " + e.message)
        }
    }

    // 发送数据
    public func send(data: Array<Byte>): Result<Int, String> {
        return try {
            if (socket.isNone()) {
                return Err("未连接到 broker")
            }

            if (tlsSocket.isSome()) {
                tlsSocket.getOrThrow().write(data)
            } else {
                socket.getOrThrow().write(data)
            }

            Ok(data.size)
        } catch (e: Exception) {
            Err("发送失败: " + e.message)
        }
    }

    // 设置接收回调
    public func onReceive(callback: (Array<Byte>) -> Unit) {
        this.receiveCallback = Some(callback)
    }

    // 启动接收线程
    private func startReceiveThread() {
        isRunning = true

        spawn {
            let buffer = Array<Byte>(4096, repeat: 0)

            while (isRunning && socket.isSome()) {
                try {
                    if (tlsSocket.isSome()) {
                        let tls = tlsSocket.getOrThrow()
                        let bytesRead = tls.read(buffer)
                        if (bytesRead > 0) {
                            let data = Array<Byte>(Int64(bytesRead), repeat: 0)
                            for (i in 0..bytesRead) { data[Int64(i)] = buffer[Int64(i)] }
                            if (receiveCallback.isSome()) {
                                receiveCallback.getOrThrow()(data)
                            }
                        } else if (bytesRead == 0) {
                            isRunning = false
                            break
                        }
                    } else {
                        let sock = socket.getOrThrow()
                        let bytesRead = sock.read(buffer)
                        if (bytesRead > 0) {
                            let data = Array<Byte>(Int64(bytesRead), repeat: 0)
                            for (i in 0..bytesRead) { data[Int64(i)] = buffer[Int64(i)] }
                            if (receiveCallback.isSome()) {
                                receiveCallback.getOrThrow()(data)
                            }
                        } else if (bytesRead == 0) {
                            isRunning = false
                            break
                        }
                    }
                } catch (e: Exception) {
                    if (isRunning) {
                        println("接收错误: " + e.message)
                    }
                    break
                }
            }
        }
    }

    // 关闭连接
    public func close() {
        isRunning = false
        try {
            if (tlsSocket.isSome()) {
                tlsSocket.getOrThrow().close()
                tlsSocket = None
            }
        } catch (e: Exception) {}
        if (socket.isSome()) {
            try {
                socket.getOrThrow().close()
            } catch (e: Exception) {}
            socket = None
        }
    }

    // 检查连接状态
    public func isConnected(): Bool {
        if (tlsSocket.isSome()) {
            return !tlsSocket.getOrThrow().isClosed() && isRunning
        }
        return socket.isSome() && isRunning
    }
}
