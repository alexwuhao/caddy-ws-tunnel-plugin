# Caddy WS Reverse Tunnel Plugin

NAT 穿透反向隧道 Caddy 插件 — 让防火墙/内网后的 agent 主动外连 Caddy,通过 WebSocket + smux 多路复用,将外部 HTTP 请求透传到 agent 本地端口。

## 特性

- **反向隧道**：client 主动外连 Caddy（NAT 友好），Caddy 通过隧道把请求发给 client 转发到本地
- **smux 多路复用**：单条 WebSocket 承载数百并发流，减少连接数
- **固定连接池**：`channels` 条数据通道，配置确定性，无动态扩缩容
- **L4 透传**：隧道层不解析 HTTP，流 = `net.Conn`，直接对接标准 `reverse_proxy`
- **least-loaded 选通道 + 背压排队**：通道打满时排队 ≤ `queue_timeout`，超时 503
- **峰值统计**：`peak_demand` / `channels_needed_at_peak` / `saturated_total` 指导离线扩容
- **流量速率指标**：`rate_in_bps` / `rate_out_bps` 由 1s 采样 × 10s 滑动窗口估算（字节/秒）
- **控制断宽限期重连**：控制通道断后 `heartbeat×3` 内保留在途流，重连恢复路由
- **可选 HMAC 鉴权**：`hmac_secret` 开启 HMAC-SHA256 挑战应答（时间戳 + nonce 防重放），默认仍是明文 key + wss
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
                # hmac_secret <secret>  # 可选：启用 HMAC 鉴权后替代明文 key（见「鉴权」）
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
    # 注意：reverse_proxy 后面必须跟一个非空 upstream（这里用占位名 tunnel）。
    # Caddy 只有在存在 upstream 时才会调用自定义 transport；留空会直接 503。
    reverse_proxy tunnel {
        transport tunnel node1:8080   # 内联写法：真正的寻址由 transport 决定
    }
}

# 等价展开写法
db.internal.example.com {
    reverse_proxy tunnel {
        transport tunnel {
            node node1
            port 5432
        }
    }
}
```

> **易错点**：`reverse_proxy` 的第一个参数是**占位 upstream**（名字随意，如 `tunnel`），它的地址不会被真正拨号；寻址由 `transport tunnel node:port` 决定。写成 `reverse_proxy { transport tunnel ... }`（不给 upstream）会被 Caddy 判定为没有上游，直接返回 **503**，transport 根本不会被调用。

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

## 客户端（tunnel-agent）

服务端需要一个主动外连的 agent 运行在 NAT/内网侧。Windows / Linux / macOS 二进制见
[**caddy-ws-tunnel-client releases**](https://github.com/alexwuhao/caddy-ws-tunnel-client/releases)
（`tunnel-agent ≥ v1.1.0` 才支持本仓库的 HMAC 模式；旧版本只能走明文 key）。

```powershell
# 明文 key 模式（兼容所有版本）
tunnel-agent-windows-amd64.exe --url wss://caddy.example.com:8443/ws --key xx1

# HMAC 模式（服务端节点配置 hmac_secret 后必须升到 v1.1.0+）
tunnel-agent-windows-amd64.exe --url wss://caddy.example.com:8443/ws --node node1 --secret "<hmac_secret>"
```

Windows 后台常驻建议用 NSSM 或任务计划程序（客户端仓库 README 有命令示例）。

## 配置参数详解

### 节点配置（`tunnel_server` 块内）

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `key` | string | 见说明 | 节点明文鉴权密钥。client 连接时必须提供相同的 key。多个 client 使用相同 key 时视为同一 node 的多个连接。<br>• 未配置 `hmac_secret` 时**必填**（比对为常量时间）<br>• 配置了 `hmac_secret` 后保留但不再参与鉴权（便于迁移，可删）<br>• **`key` 与 `hmac_secret` 至少要配一个**，两者都空会在 provision 期报错（否则空 key == 空 key，等于无鉴权） |
| `hmac_secret` | string | 空（关闭） | **可选**。非空即为该节点启用 HMAC-SHA256 挑战应答鉴权：client 的每条 hello（control 与 data 通道都要）必须带 `ts`/`nonce`/`sig`，`sig = hex(HMAC-SHA256(secret, "node\|ts\|nonce"))`；服务端校验 `\|now-ts\| ≤ 60s` 并对 nonce 防重放。详见「鉴权（可选 HMAC）」。 |
| `channels` | int | `4` | **固定数据通道数**。每条通道是一个独立的 WebSocket + smux 会话。该值决定了节点的并发容量上限（`channels × max_streams_per_conn`）。**必须 > 0**：Caddyfile 里写 `channels 0` 会直接报错，JSON 里省略或写 `0` 表示使用默认值 `4`。 |
| `max_streams_per_conn` | int | `256` | 单个数据通道的最大并发流数。调优建议：<br>• 高并发场景：增大到 `512` 或 `1024`<br>• 低延迟优先：减小到 `128`<br>• 配合 `channels` 计算总容量：`total = channels × max_streams_per_conn` |
| `port <id> [addr]` | mapping | **必填** | 端口映射清单。<br>• `id`：**必须是纯数字**（如 `8080`, `5432`），用于流头编码<br>• `addr`：本地目标地址，默认 `127.0.0.1:<id>`<br>• 示例：<br>&nbsp;&nbsp;`port 8080` → `127.0.0.1:8080`<br>&nbsp;&nbsp;`port 9090 10.0.0.2:9090` → `10.0.0.2:9090`<br>• 至少配置 1 个端口 |

### 全局配置（`tunnel` app 级 / `tunnel_server` 块内）

这三个参数有两种写法，效果相同：

- **JSON**：写在 `apps.tunnel` 下（`"heartbeat": "15s"`）；
- **Caddyfile**：写在 `tunnel_server { ... }` 块内、`node` 之外的顶层（会覆盖 app 里的值）。

```caddyfile
tunnel_server {
    heartbeat     15s
    dial_timeout  10s
    queue_timeout 5s
    node node1 { key xx1; channels 4; port 8080 }
}
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `heartbeat` | duration | `15s` | 控制通道心跳间隔。影响：<br>• **宽限期 = `heartbeat × 3`**：控制断后在此时间内重连可恢复路由<br>• 调优建议：<br>&nbsp;&nbsp;- 稳定网络：`30s`（减少带宽开销）<br>&nbsp;&nbsp;- 不稳定网络：`10s`（快速检测断连）<br>&nbsp;&nbsp;- NAT 穿透：`15s`（平衡保活与开销） |
| `dial_timeout` | duration | `10s` | 开流超时。从 `OpenStream` 到写完流头的最长时间（用 smux 写超时实现）。超时 / 开流失败返回 **502**。<br>• 调优建议：高延迟链路增大到 `20s`<br>• 注意：**不限制后端响应时间**——后端慢不会被插件掐断，需要响应超时请用 `reverse_proxy` 的 transport 超时/前端超时 |
| `queue_timeout` | duration | `5s` | 背压排队超时。所有通道打满时，请求排队等待空闲槽位的最长时间。超时返回 **503**。<br>• 调优建议：<br>&nbsp;&nbsp;- 低延迟优先：`2s`（快速失败）<br>&nbsp;&nbsp;- 高吞吐优先：`10s`（允许更长排队） |

> 生效时机：这些值在 **provision 时**读取。改完需 `caddy reload`（Caddy 的优雅 reload 会让 client 自动重连、重建固定池，期间可能有短暂 503）。

### 状态接口配置（`tunnel_status` 块内）

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `auth_token` | string | 空 | `/status` 端点的 Bearer 认证令牌。<br>• 未设置：公开访问<br>• 已设置：需要 `Authorization: Bearer <token>` 请求头<br>• 建议：生产环境设置强随机令牌 |

## 鉴权（可选 HMAC）

插件支持两种鉴权姿态，**按节点选择**，未配置 `hmac_secret` 的节点行为与之前完全一致：

| 模式 | 触发条件 | 线上传输 | 适用 |
|------|----------|----------|------|
| 明文 key（默认，兼容旧 client） | 未配置 `hmac_secret` | `hello.key` 明文 + wss 加密 | 内网、已有 client |
| HMAC 挑战应答 | 配置了 `hmac_secret` | 只传 `ts`/`nonce`/`sig`，**不传 secret** | 公网、可升级 client 的场景 |

### 配置

```caddyfile
tunnel_server {
    node node1 {
        hmac_secret "至少 32 字节的强随机字符串"   # 开启 HMAC；key 行可保留但不再参与鉴权
        channels 4
        port 8080
    }
}
```

> 不想把 secret 写进配置文件时，可以用 Caddy 的环境变量占位：`hmac_secret {$NODE1_HMAC_SECRET}`。

JSON 等价写法：

```json
{"apps": {"tunnel": {"nodes": {"node1": {
  "hmac_secret": "至少 32 字节的强随机字符串",
  "channels": 4,
  "ports": {"8080": "127.0.0.1:8080"}
}}}}}
```

### 客户端签名步骤（每条 hello 都要签）

control 通道和数据通道的 `hello` 都要带同样的三元组：

1. `nonce`：每条 hello **重新生成**的随机串（建议 16 字节 hex，长度 1..128），**不能复用**；
2. `ts`：当前 Unix 秒；
3. `sig`：`hex(HMAC-SHA256(secret, node + "|" + ts + "|" + nonce))`。

Go 客户端可直接复用协议包里的实现，避免签名串漂移：

```go
import "github.com/wjzhangq/caddy-ws-tunnel-plugin/modules/tunnel/proto"

ts, nonce := time.Now().Unix(), randHex(16)
hello := proto.Hello{
    Role: proto.RoleControl, Node: "node1",
    Ts: ts, Nonce: nonce,
    Sig: proto.SignHello(secret, "node1", ts, nonce),
}
// data 通道同理，另带 session / channel_id，并为每条 hello 生成新的 nonce。
```

### 服务端校验规则

- `|now - ts| ≤ 60s`（`proto.HMACClockSkewSeconds`），超出直接拒绝；
- `sig` 用 `hmac.Equal` 常量时间比较，十六进制非法直接拒绝；
- nonce 防重放：有界缓存 4096 条，条目保留 60s（超出时间窗的重放已由 ts 挡掉）；缓存只在签名验证通过后写入，未认证连接无法污染缓存；
- 启用了 `hmac_secret` 的节点**不再接受明文 key**（`Authenticate` 对 HMAC 节点恒为 false），不会出现降级；
- 鉴权失败时 WS close reason 固定为 `auth failed`，具体原因只写服务端日志（不向未认证对端泄露细节）。

### 鉴权失败排查

客户端只会看到通用的 close 原因 `auth failed`（1008）；具体原因在服务端 zap 日志的 `tunnel auth failed` 条目里（带 `node` / `role` / `remote`）：

| 日志原因 | 含义 | 处理 |
|---|---|---|
| `unknown node` | node 名与配置不匹配 | 检查 client 的 node 名与 Caddyfile 里的 `node <name>` |
| `hmac signature required` | HMAC 节点收到无 `sig` 的 hello | client 未升级到 HMAC 模式 |
| `hmac signature invalid` | secret 不一致或签名入参被改 | 两边 secret 必须逐字节一致 |
| `timestamp outside allowed skew` | 客户端与服务端时钟偏差 > 60s | 校时（NTP） |
| `invalid nonce` | nonce 为空或超过 128 字节 | 用 16 字节随机数的 hex |
| `nonce already used` | 重放，或数据通道重连没换新 nonce | 每条 hello 生成新 nonce |

### 兼容性与注意

- 这是**协议级可选扩展**：现有明文 client 无需改动即可继续用；要启用 HMAC 需要 `tunnel-agent` **≥ v1.1.0**（该版本起控制/数据通道 hello 均签名，且 secret 不再作为明文 key 发送）。配套客户端构建见 [caddy-ws-tunnel-client v1.1.0](https://github.com/alexwuhao/caddy-ws-tunnel-client/releases/tag/v1.1.0)（上游 `wjzhangq/caddy-ws-tunnel-client` 待同步该修复）。
- `hmac_secret` 就是共享密钥，等同于密码：请用 `openssl rand -hex 32` 之类生成，不要提交进仓库；轮换时更新 Caddy 配置与 client 侧即可（旧 secret 立即失效）。
- 建议始终配合 `wss://`（TLS），HMAC 解决的是"密码不上线 + 防重放"，不替代传输加密。

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
    "traffic": {"bytes_in": 91234567, "bytes_out": 4523110987, "rate_in_bps": 120480, "rate_out_bps": 8841200},
    "client": {"channels": 4, "streams": 47, "bytes_in": 91234567, "bytes_out": 4523100000, "local_dial_errors": 2},
    "ports": ["8080", "9090"],
    "last_error": null
  }]
}
```

> `client` 块只在 client 发过 `stats` 遥测帧后出现（server 侧累计字节以 `traffic` 为准）。

### 关键指标说明

| 字段 | 含义 | 调优建议 |
|------|------|----------|
| `peak_demand` | 峰值并发需求（活跃流 + 排队数）| 若接近 `max_concurrent`，考虑增加 `channels` |
| `channels_needed_at_peak` | 峰值时所需通道数 | 建议 `channels ≥ channels_needed_at_peak + 1`（留冗余） |
| `peak_utilization` | 峰值利用率（`peak_demand / max_concurrent`）| 超过 0.8 表示容量紧张，建议扩容 |
| `saturated_total` | 累计饱和次数（触发排队的请求数）| 持续增长表示容量不足或突发过高 |
| `data_reconnect_total` | 数据通道重连次数 | 频繁重连（> 10/小时）表示网络不稳定 |
| `queue_depth` | 当前排队深度 | 持续 > 0 表示流量超过处理能力 |
| `bytes_in` / `bytes_out` | 累计字节数（smux 之下统计，含已断开通道的历史） | 用于容量与计费核对 |
| `rate_in_bps` / `rate_out_bps` | 滑动窗口速率，**单位字节/秒（B/s，不是 bit/s）** | 1s 采样一次、10s 窗口；最后一个字节过去 ~10s 后衰减为 0 |

> 关于速率：字节计数在 smux 之下（`countingConn`），所以统计的是真实链路字节（含 smux 帧头开销），不是业务 payload。数据通道断线重连时其历史字节会折算进节点累计值，因此 `bytes_in/out` 单调不减、速率不会因重连跳变。
>
> 单位说明：字段名沿用历史的 `_bps` 后缀，但语义是 **bytes per second**；换成 bit/s 请自行 ×8。Prometheus 中用 `tunnel_bytes_rate_bps{dir="in|out"}`（help 里已注明 B/s）。

### Prometheus 指标

插件在 Caddy 的 `/metrics` 端点（开启 `admin` 或 `prometheus` 指令时）暴露以下指标，全部带 `node` 标签：

| 指标 | 类型 | 说明 |
|------|------|------|
| `tunnel_node_up` | gauge | 控制通道是否在线（1/0） |
| `tunnel_channels{state="configured\|online"}` | gauge | 配置/在线数据通道数 |
| `tunnel_streams_active` | gauge | 当前活跃流数 |
| `tunnel_streams_peak` | gauge | 峰值并发需求（含排队） |
| `tunnel_channels_needed_peak` | gauge | 峰值时理论所需通道数 |
| `tunnel_stream_open_total{result="ok\|queue_timeout\|control_down\|open_stream_error\|header_error\|..."}` | counter | 开流结果分布 |
| `tunnel_bytes_total{dir="in\|out"}` | gauge | 累计字节（B） |
| `tunnel_bytes_rate_bps{dir="in\|out"}` | gauge | 滑动窗口速率（**B/s**） |
| `tunnel_node_saturated_total` | gauge | 饱和（触发排队）累计次数 |
| `tunnel_local_dial_errors_total` | gauge | client 上报的本地 dial 失败数 |

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

> **M6 实测修正**：1.25× 只是下限；实测容量刚好覆盖并发时仍有 close-lag 排队（p99 从 ~30ms 涨到 130–260ms）。建议把目标容量提到 **≥ 1.5~2 × 预期峰值并发**，且 `channels ≥ 2`。数据见 [`docs/M6-benchmark-report.md`](docs/M6-benchmark-report.md)。

## M6 压测结论

完整方法、36 场景原始矩阵、HOL 实测与局限见 [`docs/M6-benchmark-report.md`](docs/M6-benchmark-report.md)。本机实测（Ryzen 9 9950X3D / 32 逻辑核 / Windows 11 / 全 loopback / 单进程压测台，2026-11-23，代码基线含 `peak_demand` 双计修复）：

| 结论 | 实测数据 |
|---|---|
| 单机吞吐由压测台 CPU 决定，与 `channels` 关系不大 | 512B 响应 13.4–17.0k req/s；并发 10→200 只涨 13–27% |
| 尾延迟“悬崖”由**容量是否覆盖并发**决定 | 并发 200：容量 128 → p99 192–259ms；容量 ≥256 → p99 26–34ms |
| `channels` 的收益主要在吞吐与故障隔离 | 50% 响应为 64KB 时：1 通道 6.9k → 4 通道 12.9k req/s（+87%） |
| 推荐配比 | 通用/大响应 `channels=4, max_streams_per_conn=256`；省连接 `2×256` |
| 容量公式收紧 | `channels × max_streams_per_conn ≥ 1.5~2 × 预期峰值并发` |
| 无丢包 loopback 上未观测到用户态 HOL 放大 | 10% 大响应混合时短请求 p99 5.4–5.5ms，与纯小请求基线 5.6ms 持平 |
| 错误分布 | 矩阵 503=0；0.035% 的 502 集中在测试台最高 churn 场景（stub 侧临时端口压力，判定为压测台伪影） |

> 局限：压测台把负载发生器、Caddy、client、echo 放在同一进程共享 CPU，且 loopback 无丢包/无 RTT——**绝对吞吐不代表生产上限**，但容量/并发与尾延迟的趋势可复用；TCP 丢包型 HOL 需跨机或 `tc netem` 另测。

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
          （启用 hmac_secret 时改为 node, ts, nonce, sig，key 可省）
2. server → welcome{session, heartbeat, channels, config{ports,...}}
3. client → 并发建 channels 条数据通道：hello{role:"data", session, channel_id}
          （HMAC 模式下 data hello 也要 ts/nonce/sig，nonce 每条新生成）
4. server → welcome{channel_id}；双方在该 WS 上启动 smux
5. server OpenStream → 写流头(ver+varint port_id) → HTTP 字节
6. client AcceptStream → 读流头 → 查清单 → dial 本地 → io.Copy
```

流头格式：`[ver 1B][port_id uvarint][raw HTTP bytes...]`

`proto` 包是对外契约（client 镜像实现）：
- `version.go`：`ProtocolVersion = 1`
- `messages.go`：`Envelope` / `Hello` / `Welcome` / `Config` 等控制消息
- `streamheader.go`：流头编解码
- `auth.go`：HMAC 签名契约（`CanonicalHello` / `SignHello` / `VerifyHelloSignature`，`HMACClockSkewSeconds`，`MaxNonceLen`）

## 开发

```bash
# 全量测试（含竞态检测；-race 需要 CGO 与本地 C 编译器）
go test -race ./...

# 只跑单元测试（快，无需 cgo）
go test ./modules/tunnel/...

# M6 压测基准（详见 docs/M6-benchmark-report.md）
go test ./test -run '^$' -bench . -benchmem
# Windows 一键跑矩阵并落结果：
pwsh scripts/bench.ps1

# 构建验证
xcaddy build v2.11.4 --with github.com/wjzhangq/caddy-ws-tunnel-plugin=.

# 验证 Caddyfile
./caddy validate --config test/Caddyfile.test --adapter caddyfile
```

CI（`.github/workflows/ci.yml`）在 push / PR 时执行：`gofmt` 检查 → `go build` → `go vet` → `go test -race` → `xcaddy build` 并用 `list-modules` 校验 4 个模块已注册。

### 目录结构

```
modules/tunnel/
├── proto/            # 对外协议契约（client 镜像此包）
│   ├── version.go        # ProtocolVersion = 1
│   ├── messages.go       # 控制消息结构体
│   ├── auth.go           # HMAC hello 签名契约（CanonicalHello/SignHello）
│   └── streamheader.go   # 流头编解码
├── app.go            # TunnelApp：注册表 + 健康巡检 + 速率采样 + 峰值统计
├── server.go         # /ws 接入 handler（读 hello → AuthenticateHello → 分流）
├── server_caddyfile.go   # Caddyfile 解析
├── transport.go      # reverse_proxy transport
├── dialer.go         # PickChannel → OpenStream → WriteStreamHeader
├── node.go           # Node/NodeRegistry/PickChannel/peak stats
├── channel.go        # 数据通道：WS→countingConn→smux
├── control.go        # 控制通道：welcome/ping/pong
├── attach.go         # attachControl/attachData/checkGrace/drain
├── auth.go           # 鉴权：明文 key（常量时间）+ 可选 HMAC 挑战应答 + nonce 防重放
├── rate.go           # 流量累计/退役折算 + 滑动窗口速率估算（B/s）
├── metrics.go        # Prometheus 指标
├── node_test.go      # 池/峰值单测
├── auth_test.go      # 鉴权/防重放单测
├── config_test.go    # 配置解析/校验/默认值/系统指令单测
└── rate_test.go      # 速率窗口单测
test/
├── integration_test.go   # M1–M5 验收用例（goleak 保证无泄漏）
├── bench_test.go         # M6 压测基准（见 docs/M6-benchmark-report.md）
├── stubclient/           # 最小测试桩 client（非交付件）
└── Caddyfile.test
docs/
└── M6-benchmark-report.md  # M6 实测报告（环境/矩阵/结论/复现命令）
scripts/
└── bench.ps1               # 一键跑压测并落结果（Windows）
dev.md                      # 施工手册（面向维护者）
LICENSE                     # MIT
```

## 里程碑完成度

| 里程碑 | 状态 | 说明 |
|--------|------|------|
| M1 单流打通 | ✅ | 控制握手 + 单数据通道 + 流头透传 |
| M2 多路复用 | ✅ | 50 并发 + 端口允许清单校验 |
| M3 固定连接池 | ✅ | least-loaded + 背压排队 + 峰值统计 |
| M4 transport + 状态 | ✅ | 内联/块写法 + /status JSON + 流量速率指标 |
| M5 健壮性 | ✅ | 控制断宽限重连 + 数据通道补齐 + reload + drain + metrics + HMAC |
| M6 压测调参 | ✅ | 见 [`docs/M6-benchmark-report.md`](docs/M6-benchmark-report.md) 与下方结论 |

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
- 鉴权：明文 key（默认，兼容旧 client）或 **可选 HMAC-SHA256 挑战应答**（`hmac_secret`，60s 时间窗 + nonce 防重放）
- 速率：**1s 采样 × 10s 滑动窗口**，单位 B/s；累计字节在通道退役时折算，不随重连回退

## License

MIT，见 [LICENSE](LICENSE)。
