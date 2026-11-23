# Caddy WS 反向隧道插件 — 设计文档

> 目标 Caddy:v2.11.x · 语言:Go 1.25+ · 形态:Caddy 插件(`xcaddy build` 编译,不 fork)
>
> 多路复用:WS 上跑 **smux** · 数据通道 **固定 `channels` 条**(无动态扩缩容) · client 仅需 `url + key`
>
> **实现状态(2026-11-23)**:M1–M6 已全部落地。§10 的 HMAC 挑战应答已实现为**按节点的可选模式**(Caddyfile/JSON `hmac_secret`),未配置时保持明文 key;§12 的速率估算已实现(1s 采样 × 10s 滑动窗口,单位 **B/s**);M6 压测结果见 [`docs/M6-benchmark-report.md`](docs/M6-benchmark-report.md)。

---

## 1. 概述

反向隧道插件:NAT/内网后的 **专有 client 主动外连** Caddy,维持 **1 条控制通道 + 固定多条数据通道**(数据通道用 smux 多路复用)。外部请求到达 Caddy 后,经数据通道下发给 client,由 client 转发到本地端口,响应原路返回。

**寻址模型:一个 `node` = 一台边缘 server,对外暴露若干本地端口,用 `node:port` 寻址**(如 `node1:8080`),直觉等同 `host:port`。

Caddy 侧不需要真实可达的 upstream 地址,而是暴露一个内部 Dialer:标准 `reverse_proxy` 通过自定义 transport 直接把请求送进隧道。

---

## 2. 角色与拓扑

```
        外部请求                          Caddy 进程
   ┌──────────────┐    公网入口      ┌──────────────────────────────────┐
   │  Browser/API │ ───────────────► │  Site: node1.example.com         │
   └──────────────┘   HTTP(S)        │    reverse_proxy                 │
                                     │      transport tunnel node1:8080 │
                                     │            │                     │
                                     │            ▼ DialContext         │
                                     │     ┌──────────────┐             │
                                     │     │ Node Registry│ App: tunnel │
                                     │     │  node1 固定池 │             │
                                     │     └──────┬───────┘             │
                                     │            │ 选通道 + 开流(端口 id)│
                                     └────────────┼─────────────────────┘
                                                  │  ▲ 控制通道 (1 条)
                          数据通道(固定 channels 条) │  config / 心跳 / 遥测
                              smux 多路复用 stream ▼  │
                                          ┌────────────────┐
                                          │  专有 Client    │  NAT 后,主动外连
                                          │  node1, key=xx1 │
                                          └───────┬────────┘
                                                  │ 端口 id → 本地地址
                                                  ▼
                                          127.0.0.1:8080  (client 本地服务)
```

| 角色 | 位置 | 职责 |
|---|---|---|
| **专有 Client** | 边缘 / NAT 后 | 主动外连 Caddy;维持 1 控制通道 + 固定 `channels` 数据通道;接收开流请求后 dial 本地端口并转发字节 |
| **Tunnel App 模块** | Caddy 进程内常驻 | 持有 Node 注册表与固定连接池;生命周期(Start/Stop);峰值/容量统计 |
| **Tunnel Handler / Transport** | Caddy 请求路径 | `/ws` 接入 client;`reverse_proxy` 的自定义 transport 把外部请求经隧道下发 |

约束:所有 WS 连接均由 **client 主动外连**(NAT 友好);数据通道 **smux 多路复用**;流方向 **Caddy → client**。

---

## 3. 配置

### 3.1 接入面(对外 server:client 接入 + 状态)

```caddyfile
:8443 {
    tls internal                      # client↔Caddy 建议 wss

    # 隧道接入:client 连这里
    route /ws {
        tunnel_server {
            # 每个 node = 一台边缘 server,暴露若干本地端口
            node node1 {
                key       xx1
                channels  4            # 固定数据通道数

                port 8080              # 简写 → 转发到 client 本地 127.0.0.1:8080
                port 9090             127.0.0.1:9090   # 显式写法(与上等价)
                port 5432             127.0.0.1:5432   # 数字 id 显式映射到任意本地地址
            }

            node node2 {
                key       xx2
                channels  2
                port 80
            }
        }
    }

    # 状态查看:node 接口状态 + 数据量 + 是否需要扩容
    route /status {
        tunnel_status {
            # auth_token <token>        # 可选;不带建议仅内网/管理面暴露
            # node node1                # 可选;省略=返回全部 node
        }
    }
}
```

`port` 指令语义:`port <id> [本地地址]`
- `<id>` 是 `node:id` 寻址用的键,**仅允许数字**(端口号风格,如 `8080`)。
- 省略地址时,默认 `127.0.0.1:<id>`;也可显式写任意本地地址,把数字 id 映射到非同名端口(如 `port 5432  127.0.0.1:5432`)。
- 本地地址是 **L4 的 `host:port`**(不带 scheme),client 据此 dial 本地 TCP。
- 服务端权威:`port` 映射在 Caddy 配置定义,握手时随 `welcome` 下发给 client 作为 **允许清单**;client 只 dial 清单内的端口 id,清单外直接拒绝。

### 3.2 业务入口(外部请求 → 隧道,反代)

```caddyfile
node1.example.com {
    reverse_proxy tunnel {          # 占位 upstream(必须非空,Caddy 才会调用自定义 transport)
        transport tunnel node1:8080   # 直达 node1 的 8080 端口
    }
}

# 等价的展开写法
db.internal.example.com {
    reverse_proxy tunnel {
        transport tunnel {
            node node1
            port 5432
        }
    }
}
```

`transport tunnel node1:port` = 不走 TCP,而是经隧道数据通道把请求送达 `node1` 的指定端口。`node1:8080` 中 `8080` 是端口 id,服务端解析为 client 本地地址。`reverse_proxy` 的 upstream 参数只是占位(地址不会被拨号),但**必须非空**,否则 Caddy 直接 503。

### 3.3 Client(只需 url + key)

client 是瘦 agent,**启动只要两个参数**;端口允许清单、通道数、mux 参数、超时等全部由服务端握手时下发,运行中可热更。

```bash
tunnel-agent --url wss://caddy.example.com:8443/ws --key xx1
#   或 TUNNEL_URL / TUNNEL_KEY 环境变量
```

- `--url`:接入面 WS 地址(含 `/ws`)。node 身份由 `key` 在服务端反查确定。
- `--key`:鉴权密钥;启用 HMAC(§10)时用它派生签名。

收益:所有路由/容量/端口策略集中在 Caddy 配置,边缘 agent 零配置、无需重新分发。

### 3.4 配置参数

| 参数 | 含义 | 默认 |
|---|---|---|
| `key` | node 鉴权 + 连接归属(同 key 归同 node) | 必填 |
| `channels` | **固定**数据通道数(无动态扩缩容) | `4` |
| `port <id> [addr]` | 暴露的本地端口,可多条;`<id>` **仅数字**,`addr` 省略默认 `127.0.0.1:<id>` | 必填 ≥1 |
| `max_streams_per_conn` | 单数据通道最大并发流 | `256` |
| `heartbeat` | **控制通道** `ping/pong` 间隔(数据通道保活由 smux keepalive 负责) | `15s` |
| `dial_timeout` | 开流(写流头→首字节回流)超时 | `10s` |
| `queue_timeout` | 通道全满时请求排队超时 | `5s` |

---

## 4. 连接模型

每个 node 一组连接,分两类。

### 4.1 控制通道(Control Channel)

- 每个 node **恰好 1 条**,client 启动第一条建立的 WS。
- 承载 **JSON 控制消息**,用于握手、配置下发、保活、遥测(指令见 §5)。
- 控制通道断 ⇒ 整个 node 判不健康,停止路由(见 §11)。

### 4.2 数据通道(Data Channel)

- 每个 node **固定 `channels` 条**,全部由 client 主动建立;断连重连补齐,运行期不增减。
- 每条 = 一条 WS 包成 `net.Conn` 后运行 smux;并发承载多个 stream。
- 每个 stream 在 Caddy 侧表现为一个 `net.Conn`,直接交给标准 `http.Transport`。

### 4.3 握手时序

```
Client (只持 url + key)                  Caddy(配置权威)
  │  ── WS /ws ──────────────────────────►│
  │  ── {hello, role:control, node, key,  │
  │       ts, nonce, sig} ───────────────►│  校验 key + HMAC
  │  ◄── {welcome, session, heartbeat,    │  分配 node 池
  │       mux, channels:N(固定),          │
  │       config:{ports, ...}} ───────────│  ★ 下发权威配置(端口允许清单等)
  │                                        │
  │  并发建立固定 channels 条数据通道:      │
  │  ── WS /ws ──────────────────────────►│
  │  ── {hello, role:data, node, key,     │
  │       session, channel_id} ──────────►│  注册进 node 池;双方在该 WS 启动 smux
  │  ◄── {welcome, channel_id} ───────────│
```

---

## 5. 控制指令

控制通道上的 JSON 消息(`type` 为消息类型):

| type | 方向 | 关键字段 | 用途 |
|---|---|---|---|
| `hello` | client → Caddy | `role`(control/data)、`node`、`key`、`ts`、`nonce`、`sig`、[`session`、`channel_id`] | 握手与鉴权 |
| `welcome` | Caddy → client | `session`、`heartbeat`、`mux`、`channels`、`config` | 握手应答,**内嵌下发会话与初始配置(端口允许清单等),client 只在此读初始配置** |
| `config`(结构) | — | `ports`(id→本地地址 **允许清单**)、`max_streams_per_conn`、`channels`、`window`、`dial_timeout` | 配置块的结构定义;**握手时内嵌在 `welcome.config`,不单独成帧**,仅被 `reload_config` 复用此结构 |
| `reload_config` | Caddy → client | 同 `config` 全量 | Caddy 配置变更时热推(无需重连) |
| `ping` / `pong` | 双向 | `nonce`、`ts` | 控制通道保活与 RTT(数据通道保活由 smux 自带) |
| `stats` | client → Caddy | `channels`、`streams`、`bytes_in/out`、`local_dial_errors` | client 侧遥测上报,喂给 `/status` 与指标 |
| `drain` | Caddy → client | `deadline` | 通知 client 停止接受新流、等存量流结束(优雅停机) |
| `bye` | 双向 | `reason` | 优雅关闭通告 |
| `error` | 双向 | `code`、`msg`、[`ref`] | 通用错误/拒绝(鉴权失败、未知端口、配置非法等) |

---

## 6. 多路复用(smux over WS)

### 6.1 方案

数据通道把 WS 包成 `net.Conn`,在其上跑 [`xtaci/smux`](https://github.com/xtaci/smux),直接获得多路复用 + 每流流控 + keepalive,产出 `OpenStream()/AcceptStream() → net.Conn`,对接 §8 的 Dialer 与标准 `http.Transport`。不手写帧协议;流控、窗口、RST、半关闭、保活均由 smux 负责。

> 可一行切 `hashicorp/yamux`(API 形态一致)。WS 库用 `coder/websocket`(二进制流模式,纯当字节管道)。

### 6.2 流头寻址

smux 流是裸字节管道,开流时先写一个极小流头,告知目标端口 id(**id 仅数字,用 varint 编码**):

```
Caddy 在 smux.OpenStream() 后,先写流头,再写 HTTP 字节:

 ┌──────────┬────────────────────────┬─────────────────────┐
 │ ver (1B) │ 端口 id(varint 数字)    │   HTTP 请求原始字节…  │
 └──────────┴────────────────────────┴─────────────────────┘

Client 在 smux.AcceptStream() 后:
  1. 读流头 → 得到端口 id(数字)
  2. 在下发的允许清单里解析为本地地址;不在清单 → 关流(等价 502)
  3. dial 本地地址,双向 io.Copy(smuxStream <-> localConn)
```

- **L4 透传**:流头之后是裸字节,隧道层不解析 HTTP。Caddy 把 smux 流包成 `net.Conn` 交 stdlib `http.Transport` 跑 HTTP/1.1。
- **方向**:仅 Caddy `OpenStream`、client `AcceptStream`(请求方向 Caddy→client)。

### 6.3 流生命周期

```
Caddy 侧                                  Client 侧
  │ OpenStream() → 写流头("8080") ────────►│ AcceptStream() → 读流头
  │                                         │ dial 127.0.0.1:8080
  │ http.Transport 写请求字节 ────────────►│ io.Copy 到本地连接
  │◄────────────────── 本地响应字节 ────────│ io.Copy 回流
  │ 请求结束 → Close(流) ──────────────────►│ 本地连接 EOF → 关闭
  │ smux 回收 streamID;通道并发数 -1        │
```

### 6.4 队头阻塞(HOL)

- smux 跑在单条 WS(单条 TCP)之上,**TCP 层 HOL 无法消除**——某流丢包重传会拖慢同通道其他流(任何单 TCP 多路复用方案皆然)。
- 缓解:用固定多条 WS(`channels`)把流摊开。延迟敏感时调小 `max_streams_per_conn`、调大 `channels`,用更多 TCP 换更少 HOL;合适的 `channels` 值由 §7.3 峰值统计指导。

---

## 7. 固定连接池与峰值统计

### 7.1 固定通道

- `channels` = 每个 node 数据通道 **固定数量**;client 启动建满、断连重连补齐,运行期不增减。
- **理论并发上限** = `channels × max_streams_per_conn`(示例 4 × 256 = 1024)。超出触发背压。

### 7.2 流量分配

```
固定 channels 条,新流落点:
1. 挑"当前并发流最少"的通道(least-loaded)→ 未满即用
2. 全部通道都达 max_streams_per_conn → 请求进队列,等 ≤ queue_timeout
3. 队列超时 → 503,累加 saturated_total,记录 peak_demand(§7.3)
```

### 7.3 峰值统计(判断是否要调大 channels)

去掉了自动扩容,改用离线判断信号:

- **`peak_demand`**:并发需求高水位 = `max(active_streams + queued_requests)`(进程内单调高水位,不随时间衰减,重载/重启清零),**含被排队/拒绝的部分**,反映真实想要的并发,不被容量上限截断。
- **`channels_needed_at_peak`** = `ceil(peak_demand / max_streams_per_conn)` —— 高峰时理论上需要几条通道。
  - `> channels` ⇒ 高峰打满过,建议调大 `channels`。
  - 长期 `≤ channels` ⇒ 容量充裕,可下调省连接。
- **`saturated_total`**:全通道打满、请求排队/被拒累计次数(>0 即出现过瓶颈)。

三值进 §12 指标与 `/status`。

---

## 8. Caddy 集成

```
tunnel (App)                       ← 常驻:Node 注册表 + 固定池 + 峰值统计,Start/Stop
  ├─ tunnel_server (HTTP Handler)  ← /ws 接入,分流 control/data 注册
  ├─ tunnel_status (HTTP Handler)  ← /status 状态与数据量
  ├─ NodeDialer                    ← DialContext 在数据通道开 smux 流,返回 net.Conn
  └─ http.reverse_proxy.transport.tunnel ← reverse_proxy 走隧道而非 TCP
```

### 8.1 App 模块(常驻)

```go
func init() { caddy.RegisterModule(TunnelApp{}) }

type TunnelApp struct {
    Nodes    map[string]*NodeConfig `json:"nodes,omitempty"`
    registry *NodeRegistry
    logger   *zap.Logger
}

func (TunnelApp) CaddyModule() caddy.ModuleInfo {
    return caddy.ModuleInfo{ID: "tunnel", New: func() caddy.Module { return new(TunnelApp) }}
}
func (a *TunnelApp) Provision(ctx caddy.Context) error { /* 建 registry、校验端口配置 */ return nil }
func (a *TunnelApp) Start() error { /* 心跳巡检 + 峰值/容量统计 goroutine */ return nil }
func (a *TunnelApp) Stop() error  { /* 优雅 drain、关池 */ return nil }
func (a *TunnelApp) Node(name string) (*Node, bool) { return a.registry.Get(name) }
```

### 8.2 WS 接入 Handler

```go
func init() { caddy.RegisterModule(TunnelServer{}) }

type TunnelServer struct { app *TunnelApp }

func (TunnelServer) CaddyModule() caddy.ModuleInfo {
    return caddy.ModuleInfo{ID: "http.handlers.tunnel_server",
        New: func() caddy.Module { return new(TunnelServer) }}
}
func (h *TunnelServer) Provision(ctx caddy.Context) error {
    app, err := ctx.App("tunnel"); if err != nil { return err }
    h.app = app.(*TunnelApp); return nil
}
func (h *TunnelServer) ServeHTTP(w http.ResponseWriter, r *http.Request, _ caddyhttp.Handler) error {
    c, err := websocket.Accept(w, r, nil); if err != nil { return err }
    hello, err := readHello(r.Context(), c); if err != nil { return err }
    if !h.app.Auth(hello.Node, hello.Key, hello.Sig) {
        return c.Close(websocket.StatusPolicyViolation, "auth failed")
    }
    switch hello.Role {
    case "control": return h.app.registry.AttachControl(hello.Node, c) // 下发 config(端口清单)
    case "data":    return h.app.registry.AttachData(hello.Node, hello.ChannelID, c) // 启 smux
    }
    return c.Close(websocket.StatusUnsupportedData, "unknown role")
}
```

### 8.3 内部 Dialer(把 smux 流当 net.Conn)

```go
type NodeDialer struct {
    node *Node
    port string   // 端口 id,写进流头,client 据允许清单解析
}

func (d *NodeDialer) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
    ch, err := d.node.PickChannel(ctx)        // §7.2 least-loaded,全满则排队 ≤ queue_timeout
    if err != nil { return nil, err }          // 池满/超时 → err → 上层 503
    stream, err := ch.session.OpenStream()     // smux 开流,返回 net.Conn
    if err != nil { return nil, err }
    if err := writeStreamHeader(stream, d.port); err != nil { stream.Close(); return nil, err }
    return stream, nil
}
```

### 8.4 reverse_proxy 自定义 transport

复用 stdlib `http.Transport`,只把 `DialContext` 换成隧道拨号器。支持 `node:port` 内联写法与展开块写法。

```go
func init() { caddy.RegisterModule(TunnelTransport{}) }

type TunnelTransport struct {
    Node string `json:"node"`
    Port string `json:"port"`
    app  *TunnelApp
    rt   http.RoundTripper
}

func (TunnelTransport) CaddyModule() caddy.ModuleInfo {
    return caddy.ModuleInfo{ID: "http.reverse_proxy.transport.tunnel",
        New: func() caddy.Module { return new(TunnelTransport) }}
}

// 解析 `transport tunnel node1:8080` 内联参数,或块内 node/port
func (t *TunnelTransport) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
    for d.Next() {
        if d.NextArg() {                       // 内联:node1:8080
            host, port, _ := strings.Cut(d.Val(), ":")
            t.Node, t.Port = host, port
        }
        for d.NextBlock(0) {                    // 块内:node / port
            switch d.Val() {
            case "node": d.Args(&t.Node)
            case "port": d.Args(&t.Port)
            }
        }
    }
    return nil
}

func (t *TunnelTransport) Provision(ctx caddy.Context) error {
    app, err := ctx.App("tunnel"); if err != nil { return err }
    t.app = app.(*TunnelApp)
    node, ok := t.app.Node(t.Node); if !ok { return fmt.Errorf("unknown tunnel node: %s", t.Node) }
    dialer := &NodeDialer{node: node, port: t.Port}
    t.rt = &http.Transport{DialContext: dialer.DialContext, DisableKeepAlives: true}
    return nil
}
func (t *TunnelTransport) RoundTrip(req *http.Request) (*http.Response, error) { return t.rt.RoundTrip(req) }
```

> 模块 ID 字符串与 transport 接口签名以设计意图为准,落地时对照 Caddy v2.11.x 的 `reverseproxy` 包核对(transport 注册、内联参数解析时机)。

---

## 9. 请求全链路时序

```
1. Client 启动 → 控制通道 hello/auth → welcome+config → 建固定 channels 条数据通道(WS→net.Conn→smux)
2. 外部请求 → node1.example.com → reverse_proxy → transport tunnel node1:8080
3. transport.RoundTrip → http.Transport.DialContext → NodeDialer.DialContext
4. PickChannel(并发最少)→ smux.OpenStream() → 写流头(ver, "8080")
5. Client AcceptStream() → 读流头 → 查允许清单得 127.0.0.1:8080 → dial 本地
6. http.Transport 在 net.Conn 上写 HTTP 请求 → 经 smux 发往 client
7. Client io.Copy 到本地连接 → 本地响应 → 经 smux 回流 → Caddy net.Conn.Read
8. reverse_proxy 得到 *http.Response → 回写外部
9. 请求结束 → Close(流) → smux 回收;通道并发数 -1
```

---

## 10. 鉴权与安全

- **最低**:`key` 明文握手 + `wss`(TLS)。
- **建议(HMAC 挑战,防重放,已实现为可选模式)**:client 发 `hello{node, key_id, ts, nonce, sig}`,`sig = HMAC-SHA256(secret, node|ts|nonce)`;Caddy 校验 sig、`ts` 在窗口内(±60s)、`nonce` 近期未用(有界 LRU 防重放)。节点配置 `hmac_secret` 即启用;未配置的节点保持上面的明文姿势(兼容旧 client)。实现与客户端签名契约见 `modules/tunnel/proto/auth.go`。
- **数据通道绑定**:数据通道握手须带 control 阶段的 `session`,防孤立数据连接注入。
- **端口白名单**:client 只 dial 服务端下发清单内的端口 id,清单外拒绝(防任意 dial)。
- **隔离**:不同 node 的流按命名空间严格隔离,杜绝跨 node 串流。

---

## 11. 健康、心跳、重连、故障

| 场景 | 检测 | 处理 |
|---|---|---|
| 数据通道死连接 | smux keepalive 失败 / WS 关闭 | 通道失效移出在线集;其上流中断 → 502;client 重连补回固定 `channels` 条 |
| 控制通道断开 | WS 关闭 / `ping` 超时(`heartbeat×3`) | **整个 node 判不健康**,停止接受新流(入口反代 **503**);进入 **宽限窗口**(默认 `heartbeat×3`):窗口内**保留存量数据通道与在途流**,让其继续跑完;窗口内控制重连 → 恢复路由;窗口超时 → 全断,在途流中断 → **502**,node 摘除 |
| client 整体掉线 | 控制 + 所有数据通道断 | node 摘除(或保留壳等重连);进行中请求 502 |
| 开流超时 | 写流头后 `dial_timeout` 内无字节回流 | 关流;请求 **502**(后端故障);通道连续超时降权 |
| 通道全满 | 固定通道全部达 `max_streams_per_conn` | 排队 ≤ `queue_timeout`,超时 **503**(容量不足,可重试);累加 `saturated_total`、记录 `peak_demand` |
| 端口 id 不在清单 | client 读流头查清单未命中 | client 关流;请求 502(配置/权限问题) |
| 本地端口不可达 | client 本地 dial 失败 | client 关流;请求 502,经 `stats` 计数 |

**HTTP 状态码约定(有意区分)**:
- **503** = **容量类**,语义"稍后可重试":通道全满排队超时、控制断后停新流期间。可附 `Retry-After`。
- **502** = **后端/隧道故障**,语义"上游坏了":开流/`dial_timeout` 超时、在途流被中断、端口不在清单、本地端口不可达。

**`heartbeat` 作用域**:`heartbeat`(默认 15s)**仅**用于**控制通道**的 `ping/pong` 间隔与 RTT;`ping` 超时判据为 `heartbeat×3`,控制断宽限窗口也取 `heartbeat×3`。**数据通道保活由 smux 自带 keepalive 负责,不复用此参数**。

**client 重连**:指数退避(`1s → 30s`);重连后重走控制握手并补足固定 `channels` 条。控制通道在宽限窗口内重连即恢复路由,无需中断在途流。

---

## 12. 可观测性

- **`/status`**(给人看的实时快照,`GET /status?node=node1` 可过滤):

```jsonc
{
  "nodes": [{
    "node": "node1",
    "up": true,
    "control": { "connected": true, "last_seen": "...", "rtt_ms": 3 },
    "channels": { "configured": 4, "online": 4 },
    "streams":  { "active": 47, "opened_total": 12880, "queue_depth": 0 },
    "capacity": {                       // 是否需要调大 channels
      "max_streams_per_conn": 256,
      "max_concurrent": 1024,           // channels × max_streams_per_conn
      "peak_demand": 612,               // 并发需求高水位(含排队/被拒)
      "channels_needed_at_peak": 3,     // ceil(peak_demand / max_streams_per_conn)
      "peak_utilization": 0.60,
      "saturated_total": 0
    },
    "traffic": {                        // 数据量
      "bytes_in": 91234567, "bytes_out": 4523110987,
      "rate_in_bps": 120480, "rate_out_bps": 8841200
    },
    "ports": ["8080", "9090", "5432"],
    "last_error": null
  }]
}
```

- **Prometheus 指标**:`tunnel_node_up`、`tunnel_channels{state=configured|online}`、`tunnel_streams_active`、`tunnel_streams_peak`(`peak_demand`)、`tunnel_channels_needed_peak`、`tunnel_stream_open_total{result}`、`tunnel_bytes_total{dir}`、`tunnel_node_saturated_total`、`tunnel_local_dial_errors_total`(均带 `node` 标签)。
- **字节计数**:在数据通道 WS 读写处(smux 之下)包计数 `net.Conn`,累加每 node `bytes_in/out`,速率按滑动窗口估算。

---

## 13. 优雅停机与 reload

- **stop/reload**:Caddy graceful reload 会断长连接,**依赖 client 自动重连重建固定池**;reload 瞬间有短暂 503/重连。
- **配置热更**:改端口清单 / 容量等可经 `reload_config` 在不断连情况下推给 client(不触发 Caddy 整体 reload 时)。
- **优雅 drain**:下线 node 前发 `drain`,client 停接新流、等存量流跑完再关。

---

## 14. 关键设计选择(已选定)

| 选择 | 取值 |
|---|---|
| 形态 | Caddy 插件(xcaddy),不 fork |
| 隧道方向 | 反向隧道,流由 Caddy→client 发起 |
| 多路复用 | smux over WS(可切 yamux) |
| 连接池 | 固定 `channels` 条,无动态扩缩容;容量靠峰值统计离线评估 |
| 隧道语义 | L4 原始字节透传,流=`net.Conn`,不解析 HTTP |
| 寻址 | `node:port`,端口 id 服务端权威 + 允许清单下发 |
| client 配置 | 仅 `url + key`,其余服务端下发 |
| 每请求 | 一请求一 smux 流(`DisableKeepAlives`) |
| 鉴权 | `key + HMAC 挑战` + wss |
| WebTransport | 暂不启用(WS+smux);UDP 受限场景的未来可选项 |

---

## 15. 落地里程碑

1. **M1 单流打通**:App + Handler + 控制握手 + `config` 下发;单数据通道(WS→net.Conn→smux),单流 + 流头透传 → 跑通外部请求到 client 本地端口。
2. **M2 多路复用**:smux 会话集成,单通道多流并发;流头端口寻址 + 允许清单校验。
3. **M3 固定连接池**:固定 `channels`、PickChannel(least-loaded)、背压排队、峰值统计。
4. **M4 transport + 状态接口**:`transport tunnel node:port`(内联+块)接入 `reverse_proxy`;`/status`。
5. **M5 健壮性**:心跳/重连/故障矩阵、HMAC 鉴权、Prometheus 指标。
6. **M6 压测调参**:HOL 实测、`max_streams_per_conn × channels` 配比、smux 窗口;按峰值统计定容量。**已交付**:可复现基准 `test/bench_test.go` + 实测报告 [`docs/M6-benchmark-report.md`](docs/M6-benchmark-report.md)。
