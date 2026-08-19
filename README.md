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

## 配置参数详解

### 节点配置（`tunnel_server` 块内）

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `key` | string | **必填** | 节点鉴权密钥。client 连接时必须提供相同的 key。多个 client 使用相同 key 时视为同一 node 的多个连接。 |
| `channels` | int | `4` | **固定数据通道数**。每条通道是一个独立的 WebSocket + smux 会话。该值决定了节点的并发容量上限（`channels × max_streams_per_conn`）。**不可设为 0**，启动时会校验。 |
| `max_streams_per_conn` | int | `256` | 单个数据通道的最大并发流数。调优建议：<br>• 高并发场景：增大到 `512` 或 `1024`<br>• 低延迟优先：减小到 `128`<br>• 配合 `channels` 计算总容量：`total = channels × max_streams_per_conn` |
| `port <id> [addr]` | mapping | **必填** | 端口映射清单。<br>• `id`：**必须是纯数字**（如 `8080`, `5432`），用于流头编码<br>• `addr`：本地目标地址，默认 `127.0.0.1:<id>`<br>• 示例：<br>&nbsp;&nbsp;`port 8080` → `127.0.0.1:8080`<br>&nbsp;&nbsp;`port 9090 10.0.0.2:9090` → `10.0.0.2:9090`<br>• 至少配置 1 个端口 |

### 全局配置（`tunnel` app 级）

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `heartbeat` | duration | `15s` | 控制通道心跳间隔。影响：<br>• **宽限期 = `heartbeat × 3`**：控制断后在此时间内重连可恢复路由<br>• 调优建议：<br>&nbsp;&nbsp;- 稳定网络：`30s`（减少带宽开销）<br>&nbsp;&nbsp;- 不稳定网络：`10s`（快速检测断连）<br>&nbsp;&nbsp;- NAT 穿透：`15s`（平衡保活与开销） |
| `dial_timeout` | duration | `10s` | 开流超时。从 `OpenStream` 到写完流头的最长时间。超时返回 **502**。<br>• 调优建议：高延迟链路增大到 `20s` |
| `queue_timeout` | duration | `5s` | 背压排队超时。所有通道打满时，请求排队等待空闲槽位的最长时间。超时返回 **503**。<br>• 调优建议：<br>&nbsp;&nbsp;- 低延迟优先：`2s`（快速失败）<br>&nbsp;&nbsp;- 高吞吐优先：`10s`（允许更长排队） |

### 状态接口配置（`tunnel_status` 块内）

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `auth_token` | string | 空 | `/status` 端点的 Bearer 认证令牌。<br>• 未设置：公开访问<br>• 已设置：需要 `Authorization: Bearer <token>` 请求头<br>• 建议：生产环境设置强随机令牌 |

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
      "saturated_total": 0,
      "data_reconnect_total": 5
    },
    "traffic": {"bytes_in": 91234567, "bytes_out": 4523110987, "rate_in_bps": 0, "rate_out_bps": 0},
    "ports": ["8080", "9090"],
    "last_error": null
  }]
}
```

### 关键指标说明

| 字段 | 含义 | 调优建议 |
|------|------|----------|
| `peak_demand` | 峰值并发需求（活跃流 + 排队数）| 若接近 `max_concurrent`，考虑增加 `channels` |
| `channels_needed_at_peak` | 峰值时所需通道数 | 建议 `channels ≥ channels_needed_at_peak + 1`（留冗余） |
| `peak_utilization` | 峰值利用率（`peak_demand / max_concurrent`）| 超过 0.8 表示容量紧张，建议扩容 |
| `saturated_total` | 累计饱和次数（触发排队的请求数）| 持续增长表示容量不足或突发过高 |
| `data_reconnect_total` | 数据通道重连次数 | 频繁重连（> 10/小时）表示网络不稳定 |
| `queue_depth` | 当前排队深度 | 持续 > 0 表示流量超过处理能力 |

> 注：`rate_in_bps` / `rate_out_bps` 当前恒为 0（滑动窗口速率估算待实现）；累计 `bytes_in/out` 已可用。

## 配置示例

### 基础配置（单节点）

```json
{
  "apps": {
    "tunnel": {
      "heartbeat": "15s",
      "dial_timeout": "10s",
      "queue_timeout": "5s",
      "nodes": {
        "backend1": {
          "key": "secret123",
          "channels": 4,
          "max_streams_per_conn": 256,
          "ports": {
            "8080": "127.0.0.1:8080"
          }
        }
      }
    },
    "http": {
      "servers": {
        "tunnel": {
          "listen": [":443"],
          "routes": [{
            "match": [{"path": ["/ws"]}],
            "handle": [{"handler": "tunnel_server"}]
          }, {
            "match": [{"path": ["/status"]}],
            "handle": [{"handler": "tunnel_status", "auth_token": "monitor-token-abc"}]
          }]
        }
      }
    }
  }
}
```

### 高并发配置

适用场景：高流量 API 网关、数据库代理

```json
{
  "apps": {
    "tunnel": {
      "heartbeat": "20s",
      "dial_timeout": "15s",
      "queue_timeout": "10s",
      "nodes": {
        "api-gateway": {
          "key": "high-throughput-key",
          "channels": 8,
          "max_streams_per_conn": 512,
          "ports": {
            "8080": "127.0.0.1:8080",
            "8081": "127.0.0.1:8081"
          }
        }
      }
    }
  }
}
```

**容量计算：**
- 最大并发流：`8 × 512 = 4096`
- 峰值利用率目标：`< 80%`
- 实际可用：`~3200` 并发连接

### 低延迟配置

适用场景：游戏服务器、实时通信

```json
{
  "apps": {
    "tunnel": {
      "heartbeat": "10s",
      "dial_timeout": "5s",
      "queue_timeout": "2s",
      "nodes": {
        "game-server": {
          "key": "low-latency-key",
          "channels": 4,
          "max_streams_per_conn": 128,
          "ports": {
            "7777": "127.0.0.1:7777",
            "7778": "127.0.0.1:7778"
          }
        }
      }
    }
  }
}
```

**优化要点：**
- 更短的心跳间隔（10s）快速检测断连
- 更小的超时（2s 排队，5s 拨号）快速失败
- 较小的 `max_streams_per_conn`（128）降低单通道延迟抖动

### 多节点配置

适用场景：多租户、多区域部署

```json
{
  "apps": {
    "tunnel": {
      "heartbeat": "15s",
      "dial_timeout": "10s",
      "queue_timeout": "5s",
      "nodes": {
        "tenant-alice": {
          "key": "alice-secret-xyz",
          "channels": 4,
          "max_streams_per_conn": 256,
          "ports": {
            "8080": "10.0.1.10:8080",
            "5432": "10.0.1.11:5432"
          }
        },
        "tenant-bob": {
          "key": "bob-secret-abc",
          "channels": 2,
          "max_streams_per_conn": 128,
          "ports": {
            "8080": "10.0.2.10:8080"
          }
        },
        "us-east": {
          "key": "region-us-east",
          "channels": 8,
          "max_streams_per_conn": 512,
          "ports": {
            "8080": "127.0.0.1:8080",
            "9090": "127.0.0.1:9090"
          }
        }
      }
    }
  }
}
```

**路由隔离：**
- 每个节点独立的连接池（固定 `channels` 数）
- 不同租户/区域互不影响
- 使用不同的 `key` 区分节点

### NAT 穿透配置

适用场景：家庭网络、公司内网穿透

```json
{
  "apps": {
    "tunnel": {
      "heartbeat": "15s",
      "dial_timeout": "20s",
      "queue_timeout": "8s",
      "nodes": {
        "home-server": {
          "key": "nat-traversal-key",
          "channels": 3,
          "max_streams_per_conn": 256,
          "ports": {
            "22": "192.168.1.100:22",
            "3389": "192.168.1.101:3389",
            "8080": "192.168.1.100:8080"
          }
        }
      }
    }
  }
}
```

**关键要点：**
- `heartbeat: 15s` 保持 NAT 映射活跃
- `dial_timeout: 20s` 容忍高延迟网络
- 宽限期：`15s × 3 = 45s`，足够网络波动恢复

## 性能调优指南

### 容量规划公式

```
最大并发流 = channels × max_streams_per_conn

建议配置：
- 预期峰值流量 ÷ 0.8 = 目标容量
- 目标容量 = channels × max_streams_per_conn
```

**示例：** 预期峰值 1000 并发
```
目标容量 = 1000 ÷ 0.8 = 1250
配置方案 A: channels=5, max_streams_per_conn=256  → 1280
配置方案 B: channels=3, max_streams_per_conn=512  → 1536
```

**选择建议：**
- 方案 A（更多通道）：故障隔离更好，单通道问题影响面小
- 方案 B（更大通道）：连接数少，握手开销低

### 超时参数调优

| 场景 | heartbeat | dial_timeout | queue_timeout |
|------|-----------|--------------|---------------|
| 本地网络（千兆局域网）| `20s` | `5s` | `3s` |
| 公网稳定（云服务商）| `15s` | `10s` | `5s` |
| 移动网络/高延迟 | `10s` | `20s` | `10s` |
| NAT 穿透/家庭网络 | `15s` | `20s` | `8s` |

### 宽限期（Grace Period）计算

**公式：** `grace_period = heartbeat × 3`

| heartbeat | grace_period | 适用场景 |
|-----------|--------------|----------|
| `10s` | `30s` | 快速故障转移，不容忍长时间断连 |
| `15s` | `45s` | 平衡：容忍短暂网络抖动 |
| `20s` | `60s` | 高容忍：适合不稳定网络 |
| `30s` | `90s` | 极高容忍：卫星链路、跨洋专线 |

**宽限期内行为：**
- ✅ 控制断连后，数据通道继续服务现有流
- ✅ **新请求返回 503**（控制断开）而非 502
- ✅ Client 重连成功后立即恢复路由，无需重建数据通道
- ❌ 宽限期过期后，强制清理所有通道和会话

### 监控告警阈值建议

基于 `/status` 接口设置：

| 指标 | 告警阈值 | 严重级别 | 说明 |
|------|----------|----------|------|
| `peak_utilization` | `> 0.8` | ⚠️ Warning | 容量紧张，考虑扩容 |
| `peak_utilization` | `> 0.95` | 🚨 Critical | 濒临饱和，立即扩容 |
| `saturated_total` 增速 | `> 10/min` | ⚠️ Warning | 频繁排队，容量不足 |
| `data_reconnect_total` 增速 | `> 5/hour` | ⚠️ Warning | 网络不稳定 |
| `channels.online` / `channels.configured` | `< 0.75` | 🚨 Critical | 通道大量离线 |
| `control.connected` | `false` 持续 `> grace_period` | 🚨 Critical | 控制通道长时间断连 |
| `queue_depth` | `> 0` 持续 `> 30s` | ⚠️ Warning | 持续背压，流量超载 |

### 故障排查清单

#### 症状：502 Bad Gateway

**可能原因：**
1. `dial_timeout` 超时 → 增大超时或检查目标服务响应
2. 目标地址不可达 → 检查 `ports` 配置和本地服务状态
3. smux 流建立失败 → 查看 client 日志

#### 症状：503 Service Unavailable

**可能原因：**
1. 控制通道断连 → 检查 client 连接状态和网络
2. 所有数据通道打满 + `queue_timeout` 超时 → 增大 `channels` 或 `max_streams_per_conn`
3. `/status` 显示 `saturated_total` 持续增长 → 容量不足

#### 症状：高 `data_reconnect_total`

**可能原因：**
1. 网络不稳定（WiFi、移动网络）
2. NAT 映射过期（heartbeat 间隔过长）
3. Client 侧频繁重启

**解决方案：**
- 减小 `heartbeat`（如 `10s`）保持连接活跃
- 检查防火墙/NAT 超时配置
- 启用 TCP keepalive（client 侧）

#### 症状：峰值利用率低但仍 503

**可能原因：** 流量分布不均，少数通道打满

**解决方案：**
- 检查 `least-loaded` 选择逻辑是否生效
- 增大 `queue_timeout` 允许更长排队
- 考虑重启 client 重新平衡连接

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
