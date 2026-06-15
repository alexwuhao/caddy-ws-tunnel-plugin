# Caddy WS Reverse Tunnel Plugin

NAT 穿透反向隧道 Caddy 插件 — 让防火墙/内网后的 agent 主动外连 Caddy,通过 WebSocket + smux 多路复用,将外部 HTTP 请求透传到 agent 本地端口。

## 特性

- **反向隧道**：client 主动外连 Caddy（NAT 友好），Caddy 通过隧道把请求发给 client 转发到本地
- **smux 多路复用**：单条 WebSocket 承载数百并发流，减少连接数
- **固定连接池**：`channels` 条数据通道，配置确定性，无动态扩缩容
- **L4 透传**：隧道层不解析 HTTP，流 = `net.Conn`，直接对接标准 `reverse_proxy`
- **least-loaded 选通道 + 背压排队**：通道打满时排队 ≤ `queue_timeout`，超时 503
- **峰值统计**：`peak_demand` / `channels_needed_at_peak` / `saturated_total` 指导离线扩容
- **控制断宽限期重连**：控制通道断后 `heartbeat×3` 内保留在途流，重连恢复路由
- **Prometheus 指标**：所有指标带 `node` 标签
- **热更新**：`reload_config` 推送端口清单/容量参数，不断连生效
- **优雅停机**：`drain` 通知 client 停新流、等存量完成

## 架构

```
外部请求 → Caddy reverse_proxy → transport tunnel node1:8080
                                      │
                                      ▼  DialContext
                              Node Registry (固定池)
                                      │  PickChannel → OpenStream → 写流头
                                      ▼
                         数据通道 (WS → smux)  ◄──── client 主动外连
                                      │
                                      ▼
                              client 本地 127.0.0.1:8080
```

## 快速开始

### 编译（xcaddy）

```bash
go install github.com/caddyserver/xcaddy/cmd/xcaddy@latest
xcaddy build v2.11.4 --with github.com/wjzhangq/caddy-ws-tunnel-plugin=.
```

### 配置（Caddyfile）

```caddyfile
# 接入面：client 连接 + 状态查看
:8443 {
    tls internal

    route /ws* {
        tunnel_server {
            node node1 {
                key       xx1           # 鉴权密钥
                channels  4             # 固定数据通道数
                port 8080               # 暴露端口，默认转发 127.0.0.1:8080
                port 9090 10.0.0.2:9090 # 显式映射到指定地址
            }
        }
    }

    route /status* {
        tunnel_status {
            # auth_token <token>  # 可选：Bearer 认证
        }
    }
}

# 业务入口：外部请求经隧道到达 client 本地
node1.example.com {
    reverse_proxy {
        transport tunnel node1:8080   # 内联写法
    }
}

# 等价展开写法
db.internal.example.com {
    reverse_proxy {
        transport tunnel {
            node node1
            port 5432
        }
    }
}
```

### JSON 配置

```json
{
  "apps": {
    "tunnel": {
      "heartbeat": 15000000000,
      "nodes": {
        "node1": {
          "key": "xx1",
          "channels": 4,
          "max_streams_per_conn": 256,
          "ports": {"8080": "127.0.0.1:8080", "9090": "10.0.0.2:9090"}
        }
      }
    }
  }
}
```

## 配置参数

| 参数 | 含义 | 默认值 |
|------|------|--------|
| `key` | 节点鉴权密钥（同 key 归同 node） | 必填 |
| `channels` | 固定数据通道数（无动态扩缩容） | `4` |
| `max_streams_per_conn` | 单通道最大并发流 | `256` |
| `port <id> [addr]` | 暴露的端口 id → 本地地址；id 仅允许数字 | 必填 ≥1 |
| `heartbeat` | 控制通道 ping 间隔（全局 app 级） | `15s` |
| `dial_timeout` | 开流超时 | `10s` |
| `queue_timeout` | 通道全满排队超时 | `5s` |
| `auth_token` | `/status` 端点的 Bearer 认证令牌 | 空（不认证） |

## Caddy 模块

| 模块 ID | 类型 | 职责 |
|---------|------|------|
| `tunnel` | App | 常驻：注册表、固定池、峰值统计、健康巡检 |
| `http.handlers.tunnel_server` | Handler | `/ws` 接入分流 control/data |
| `http.handlers.tunnel_status` | Handler | `/status` JSON 快照 |
| `http.reverse_proxy.transport.tunnel` | Transport | 把 reverse_proxy 请求经隧道发出 |

## 状态接口

`GET /status` 或 `GET /status?node=node1` 返回：

```json
{
  "nodes": [{
    "node": "node1",
    "up": true,
    "control": {"connected": true, "last_seen": "2024-...", "rtt_ms": 3},
    "channels": {"configured": 4, "online": 4},
    "streams": {"active": 47, "opened_total": 12880, "queue_depth": 0},
    "capacity": {
      "max_streams_per_conn": 256,
      "max_concurrent": 1024,
      "peak_demand": 612,
      "channels_needed_at_peak": 3,
      "peak_utilization": 0.60,
      "saturated_total": 0
    },
    "traffic": {"bytes_in": 91234567, "bytes_out": 4523110987, "rate_in_bps": 0, "rate_out_bps": 0},
    "ports": ["8080", "9090"],
    "last_error": null
  }]
}
```

> 注：`rate_in_bps` / `rate_out_bps` 当前恒为 0（滑动窗口速率估算待实现）；累计 `bytes_in/out` 已可用。

## HTTP 状态码

| 代码 | 含义 | 触发场景 |
|------|------|---------|
| **503** | 容量类，可重试 | 通道全满排队超时；控制断后停新流期 |
| **502** | 后端/隧道故障 | 开流超时；在途流中断；端口不在清单；本地不可达 |

## 协议契约

外部 client 对照 `modules/tunnel/proto/` 子包实现。核心时序：

```
1. client → hello{role:"control", node, key}
2. server → welcome{session, heartbeat, channels, config{ports,...}}
3. client → 并发建 channels 条数据通道：hello{role:"data", session, channel_id}
4. server → welcome{channel_id}；双方在该 WS 上启动 smux
5. server OpenStream → 写流头(ver+varint port_id) → HTTP 字节
6. client AcceptStream → 读流头 → 查清单 → dial 本地 → io.Copy
```

流头格式：`[ver 1B][port_id uvarint][raw HTTP bytes...]`

## 开发

```bash
# 运行测试（含 race 检测）
go test -race ./...

# 构建验证
xcaddy build v2.11.4 --with github.com/wjzhangq/caddy-ws-tunnel-plugin=.

# 验证 Caddyfile
./caddy validate --config test/Caddyfile.test --adapter caddyfile
```

### 目录结构

```
modules/tunnel/
├── proto/            # 对外协议契约（client 镜像此包）
│   ├── version.go        # ProtocolVersion = 1
│   ├── messages.go       # 控制消息结构体
│   └── streamheader.go   # 流头编解码
├── app.go            # TunnelApp：注册表 + 健康巡检 + 峰值统计
├── server.go         # /ws 接入 handler
├── server_caddyfile.go   # Caddyfile 解析
├── transport.go      # reverse_proxy transport
├── dialer.go         # PickChannel → OpenStream → WriteStreamHeader
├── node.go           # Node/NodeRegistry/PickChannel/peak stats
├── channel.go        # 数据通道：WS→countingConn→smux
├── control.go        # 控制通道：welcome/ping/pong
├── attach.go         # attachControl/attachData/checkGrace/drain
├── auth.go           # 鉴权（当前 plaintext key；HMAC seam 预留）
├── metrics.go        # Prometheus 指标
└── node_test.go      # 单元测试
test/
├── integration_test.go   # M1–M5 验收用例（goleak 保证无泄漏）
├── stubclient/           # 最小测试桩 client（非交付件）
└── Caddyfile.test
```

## 里程碑完成度

| 里程碑 | 状态 | 说明 |
|--------|------|------|
| M1 单流打通 | ✅ | 控制握手 + 单数据通道 + 流头透传 |
| M2 多路复用 | ✅ | 50 并发 + 端口允许清单校验 |
| M3 固定连接池 | ✅ | least-loaded + 背压排队 + 峰值统计 |
| M4 transport + 状态 | ✅ | 内联/块写法 + /status JSON |
| M5 健壮性 | ✅ | 控制断宽限重连 + 数据通道补齐 + reload + drain + metrics |
| M6 压测调参 | ⏳ | 待人工压测出报告 |

## 设计决策

详见 `caddy-ws-tunnel-plugin-design.md`（权威）和 `dev.md`（施工手册）。

关键选择：
- 端口 id **仅数字**，varint 编码
- config **只在 welcome 内**，不单独成帧
- 控制断 **宽限期重连**（`heartbeat×3`）
- 503/502 **有意区分**
- **固定连接池**，无动态扩缩容
- 一请求一流（`DisableKeepAlives: true`）
- L4 透传，隧道层不解析 HTTP
- 鉴权当前为 plaintext key + wss；HMAC 挑战预留接口

## License

MIT
