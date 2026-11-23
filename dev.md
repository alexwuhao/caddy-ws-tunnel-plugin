# Caddy WS 反向隧道插件 — 开发手册（dev.md）

> **面向**：本仓库的开发者、维护者、代码评审者。
> **读者分工**：`README.md` 面向*使用者*（怎么配、怎么调、怎么排障）；本文面向*改代码的人*（代码在哪、为什么这样写、改了要知道什么）。
> **权威顺序**：`caddy-ws-tunnel-plugin-design.md`（设计意图）→ 实际代码（最终事实）→ 本文（实现导读）。三者冲突时以代码为准，并请顺手更新本文。
> **目标运行时**：Caddy v2.11.4（`go.mod`）、Go 1.25.4、`proto.ProtocolVersion = 1`。
> **状态快照**：2026-11-23 **定稿**（第三轮同步：M6 报告落地、wire 超时下发、鉴权强制、CI、目录补齐）。当前能力状态见 [§0.3](#03-状态快照定稿)；剩余待办见 [§16](#16-状态快照与待办)。

---

## 0. 本文的定位与阅读路径

### 0.1 建议阅读路径

| 你的目的 | 读哪几节 |
|---|---|
| 刚接手，想知道每个文件干什么 | §2 → §3 |
| 要改协议 / 换 mux / 加指令 | §4 → §5 → §13 |
| 要调容量、看峰值指标怎么算 | §6 → §10 |
| 要排查"为什么 503 / 502"、"宽限期为什么没生效" | §7 → §14 → §8 |
| 要加配置项、加模块 | §9 → §13 |
| 要跑测试、加测试 | §12 |

### 0.2 代码注释里的 `dev.md §x` 锚点

源码里有三处注释引用了本文的小节号，它们是本文的**硬约束**，改章节号前先改注释：

| 引用位置 | 原文 | 对应本文 |
|---|---|---|
| `modules/tunnel/node.go` 顶部常量块 | `Defaults (design §3.4 / dev.md §9)` | [§9 配置参数与默认值](#9-配置参数与默认值) |
| `modules/tunnel/proto/version.go` | `Current = 1 (design §6.2 / dev.md §4.3)` | [§4.3 流头格式与端口寻址](#43-流头格式与端口寻址) |
| `modules/tunnel/proto/streamheader.go` | `(design §6.2, dev.md §4.3)` 与 `(dev.md §8 easy-error list)` | [§4.3](#43-流头格式与端口寻址) 与 [§8 常见坑](#8-常见坑与易错清单) |

### 0.3 状态快照（定稿）

| 能力 | 状态 | 证据（可 grep） |
|---|---|---|
| HMAC-SHA256 鉴权 + 端到端用例 | **已合入** | `auth.go`: `AuthenticateHello` / `authenticateHMAC` / `nonceCache`；`proto/auth.go`: `CanonicalHello` / `SignHello` / `VerifyHelloSignature`；`stubclient.Client.HMACSecret`；`TestM5_HMACHelloEndToEnd` |
| 速率指标（`rate_in_bps` / `rate_out_bps` 真实值） | **已合入** | `rate.go`: `rateMeter` / `rateWindowSeconds` / `Rates` / `TrafficTotals`；`metrics.go`: `tunnel_bytes_rate_bps`；`TestM4_TrafficRateMetrics` |
| 超时可配 + **真实值下发 client** | **已合入** | `app.go`: `DialTimeout` / `QueueTimeout` / `dialTimeout()` / `queueTimeout()`；`server.go` 三字段；`node.go`: `toProtoConfig(dialTimeout, queueTimeout)`；`attach.go`: `wireCfg`；`TestNodeProtoConfigCarriesTimeouts` |
| 配置校验（ports/channels/max/数字 id + **key 或 hmac_secret 二选一**） | **已合入** | `app.go`: `validateNodeConfig`；`server_caddyfile.go` 解析期拒绝显式 0；`TestTunnelServerRejectsUnauthenticatedNode`、`TestTunnelServerRejectsZeroChannels` |
| 指标与 `stats` 接线 | **已合入** | `dialer.go`: `streamOpenResult` / `countStreamOpen`；`control.go` → `cc.onStats` → `node.go`: `applyStats`；`metrics.go`: `localDialErrors` / `bytesRate` |
| 竞态修复（原子读 + wire config 锁内构建） | **已合入** | `metrics.go` / `status.go` 用 `atomic.LoadInt64`；`attach.go` 在 `n.mu` 内置 `wireCfg`，`toProtoConfig` 拷贝 `Ports` |
| M6 压测 harness **+ 实测报告** | **已产出** | [`test/bench_test.go`](test/bench_test.go)、[`scripts/bench.ps1`](scripts/bench.ps1)、[`docs/M6-benchmark-report.md`](docs/M6-benchmark-report.md)（36 场景矩阵 + HOL + 推荐 + 复现 + 局限） |
| CI 与构建产物忽略 | **已就绪** | [`.github/workflows/ci.yml`](.github/workflows/ci.yml)（gofmt → build → vet → `go test -race` → xcaddy list-modules）；`.gitignore` 已含 `/caddy-tunnel.exe`、`/caddy`、`/dist/` |
| `LICENSE` / `dev.md` | **已合入** | 仓库根目录 |

> 维护约定：代码已定稿，本文按当前代码表述；若后续改动导致不一致，**以代码为准**并顺手改本文。前几轮记录的差异（超时不可配、`channels=0` 校验不可达、`ports` 空清单不报错、非原子读竞态、指标无写入点、wire 超时常量、无鉴权节点、M6 报告缺失）均已修复，历史见 git log。

---

## 1. 项目定位、边界与非目标

一个 **Caddy v2 插件**（不 fork Caddy，`xcaddy build --with ...=.` 编译进主程序），把 NAT/内网后的 client 主动外连的 WebSocket 通道变成 `reverse_proxy` 可用的 upstream：

- **反向隧道**：连接方向永远是 **client → Caddy**（NAT 友好）。业务流的开放方向是 **Caddy → client**（Caddy 开流、client 接流）。
- **一条控制通道 + 固定 `channels` 条数据通道**；数据通道是 `WS → net.Conn → smux`，单条 WS 承载多路流。
- **L4 透传**：隧道层不看 HTTP，只搬字节；HTTP 语义交给标准 `http.Transport`。
- **Caddy 是配置权威**：端口允许清单、通道数、mux 参数、超时全部由 Caddy 下发；client 只需 `url + key`。

**明确不做**（改代码前请先认同这些边界，很多设计取舍由此而来）：

| 非目标 | 原因 / 替代 |
|---|---|
| 动态扩缩容 | 固定池便于容量确定与故障隔离；扩容信号交给峰值统计做**离线决策**（§6） |
| 在隧道层解析 HTTP（路由/改写/缓存） | 保持 L4，任何应用层协议都能过；需要 HTTP 能力就用 Caddy 的 handler 链 |
| 正向代理 / 多跳 | 只做 `node:port` 这一种寻址 |
| 客户端配置持久化 | client 每次都从 `welcome.config` 拿全量配置 |
| 隧道内 UDP | 当前只用 WS+TCP+smux |

**硬约束**（都是刻意的）：

1. 端口 id **只能是数字**，因为它要进 uvarint 流头（§4.3）。
2. `config` **不单独成帧**，只内嵌在 `welcome` 里；`reload_config` 复用同一个结构体（§4.4）。
3. 控制通道断 → **新流立刻 503**，但**在途流继续跑完**（宽限期，§7）。
4. 503/502 语义有意区分：**503 = 容量/可重试，502 = 后端/隧道故障**（§5.3、§7.5）。
5. 一请求一流（`DisableKeepAlives: true`），不做连接复用。

---

## 2. 目录结构与文件职责

```
caddy-ws-tunnel-plugin/
├── plugin.go                      # 顶层包 caddywstunnel：blank import 触发模块注册
├── modules/tunnel/
│   ├── app.go                     # TunnelApp：注册表 / 生命周期 / 巡检与采样 goroutine
│   ├── node.go                    # Node / NodeConfig / NodeRegistry / PickChannel / 峰值统计
│   ├── channel.go                 # countingConn / dataChannel（smux 会话）/ streamConn
│   ├── control.go                 # controlChannel：写序列化 / ping-pong / RTT / readLoop
│   ├── attach.go                  # attachControl / attachData / checkGrace / reload / drain / shutdown
│   ├── dialer.go                  # NodeDialer.DialContext：PickChannel → OpenStream → 写流头
│   ├── transport.go               # TunnelTransport：reverse_proxy 自定义 transport + 状态码映射
│   ├── server.go                  # TunnelServer：/ws 接入、hello 解析、鉴权、按 role 分流
│   ├── server_caddyfile.go        # Caddyfile：tunnel_server / tunnel_status 指令解析
│   ├── status.go                  # TunnelStatus：/status JSON 快照
│   ├── metrics.go                 # Prometheus 指标（全部带 node 标签）
│   ├── auth.go                    # 鉴权：HMAC 校验 + 明文 key 兼容 + nonce 防重放 + session 生成
│   ├── rate.go                    # 字节累计 / 退役累加 / 滑动窗口速率估算
│   ├── node_test.go               # 池选择、峰值、默认值、宽限期单测
│   ├── auth_test.go               # HMAC / 明文鉴权、时钟偏移、重放、并发、Caddyfile/JSON 配置单测
│   ├── config_test.go             # 全局指令解析、非法配置拒绝、validateNodeConfig、applyStats
│   ├── rate_test.go               # rateMeter 与流量累计单测
│   └── proto/                     # ★ 对外协议契约：client 仓库镜像此包
│       ├── version.go             # ProtocolVersion = 1
│       ├── messages.go            # 消息类型常量 + Envelope/Hello/Welcome/Config/...
│       ├── streamheader.go        # 流头编解码（ver + uvarint port id）
│       ├── auth.go                # HMAC 签名串规范：CanonicalHello / SignHello / VerifyHelloSignature
│       └── streamheader_test.go   # 流头边界与 envelope 单测
├── test/
│   ├── integration_test.go        # M1–M5 端到端用例（goleak 保证无泄漏）
│   ├── bench_test.go              # M6 压测 harness（固定窗口矩阵 + HOL + echo 直连基线）
│   ├── stubclient/                # 最小测试桩 client（非交付件，见 §12.4）
│   └── Caddyfile.test             # caddy validate 用的示例配置
├── scripts/
│   └── bench.ps1                  # M6 基准 runner（Windows/PowerShell 7，输出 M6CSV）
├── docs/
│   └── M6-benchmark-report.md     # M6 实测报告（环境/矩阵/HOL/推荐/复现/局限）
├── .github/workflows/
│   ├── ci.yml                     # push/PR：gofmt → build → vet → go test -race → xcaddy 校验模块
│   └── release.yml                # tag v* → xcaddy 构建 linux/amd64+arm64 → Release
├── caddy-ws-tunnel-plugin-design.md  # 权威设计文档
├── README.md                      # 使用者文档
├── dev.md                         # 本文
├── LICENSE                        # MIT
├── go.mod / go.sum
└── .gitignore                     # 含构建产物（/caddy-tunnel.exe、/caddy、/dist/）
```

**逐文件职责与关键符号**

| 文件 | 职责 | 关键符号 |
|---|---|---|
| `plugin.go` | 让 `xcaddy --with` 引入即注册全部模块 | `package caddywstunnel`、`_ "…/modules/tunnel"` |
| `app.go` | 常驻 App：建表、校验、超时解析、启停后台协程、对外 `Node()` 查询 | `TunnelApp`、`Provision`、`Start`、`Stop`、`Node`、`heartbeat`、`dialTimeout`、`queueTimeout`、`healthSweep`、`rateSampler`、`validateNodeConfig`、`validatePorts`、`LastApp`、`lastApp` |
| `node.go` | 单节点运行时：控制/数据通道集合、会话、选通道、背压、峰值、wire config 组装、client 遥测 | `Node`、`NodeConfig`、`toProtoConfig`、`PickChannel`、`leastLoaded`、`recordPeak`、`recordSaturated`、`applyStats`、`dialTimeout` / `queueTimeout`（Node 字段，仅用于下发）、`ErrQueueTimeout`、`ErrNodeDownNewStream`、`ErrNoOnlineChannels`、`Default*` |
| `channel.go` | 一条数据通道 = 计数 Conn + smux Server 会话；槽位预留/释放 | `countingConn`、`dataChannel`、`newDataChannel`、`reserve`、`release`、`online`、`trafficBytes`、`streamConn` |
| `control.go` | 控制 WS 的并发安全写、心跳、RTT、消息分发 | `controlChannel`、`newControlChannel`、`sendEnvelope`、`readLoop`、`pingLoop`、`touch`、`rtt` |
| `attach.go` | 握手落地的四件事：挂控制、挂数据、宽限巡检、reload/drain | `attachControl`、`attachData`、`checkGrace`、`pushReloadConfig`、`PushReloadConfig`、`Reconfigure`、`sendDrain`、`shutdown`、`smuxConfig` |
| `dialer.go` | 把 `DialContext` 变成"选通道+开流+限时写流头"，并按结果打点 | `NodeDialer`、`DialContext`、`streamOpenResult`、`countStreamOpen` |
| `transport.go` | 接进 `reverse_proxy`；把 error 翻成 503/502；不设响应超时 | `TunnelTransport`、`UnmarshalCaddyfile`、`Provision`、`RoundTrip`、`newStatusResponse` |
| `server.go` | `/ws` 入口：升级、读 hello、鉴权、按 role 分流；Caddyfile 全局 tunables 的载体 | `TunnelServer`、`ServeHTTP`、`readHello`、`Heartbeat` / `DialTimeout` / `QueueTimeout` |
| `server_caddyfile.go` | Caddyfile 指令注册与解析（节点、状态块） | `parseTunnelServerCaddyfile`、`parseNodeBlock`、`parseTunnelStatusCaddyfile` |
| `status.go` | `/status` JSON（人类可读快照） | `TunnelStatus`、`ServeHTTP` |
| `metrics.go` | Prometheus collector 与刷新 | `metrics`、`newMetrics`、`update`、`registerOrExisting` |
| `auth.go` | 两种鉴权姿态 + 防重放 + 会话铸造 | `AuthenticateHello`、`authenticateHMAC`、`authenticatePlaintext`、`Authenticate`、`nonceCache`、`hmacSkew`、`nonceCacheSize`、`generateSession` |
| `rate.go` | 字节累计口径与速率估算 | `rateMeter`、`rateWindowSeconds`、`rateSampleInterval`、`Rates`、`TrafficTotals`、`retireChannelLocked`、`sampleTraffic` |
| `config_test.go` | 全局指令解析、非法值拒绝、`validateNodeConfig`（含鉴权二选一）、wire 超时、`applyStats` 单测 | `TestTunnelServerGlobalDirectives`、`TestTunnelServerRejectsZeroChannels`、`TestTunnelServerRejectsNodeWithoutPorts`、`TestTunnelServerRejectsUnauthenticatedNode`、`TestValidateNodeConfig`、`TestNodeProtoConfigCarriesTimeouts`、`TestAppTimeoutDefaultsAndOverrides`、`TestNodeApplyStats` |
| `test/bench_test.go` | M6 固定窗口压测：容量矩阵 / HOL / 直连基线，输出 `M6CSV` 行 | `BenchmarkTunnelCapacity`、`BenchmarkTunnelHOL`、`BenchmarkEchoDirectBaseline`、`m6RunLoad`、`m6EmitRow` |
| `scripts/bench.ps1` | 压测 runner：准备 PATH、选矩阵、跑 `go test -bench`、tee 日志 | 参数 `-Mode` / `-OutFile` / `-Window` / `-Warmup` / `-GoBin` |
| `docs/M6-benchmark-report.md` | M6 实测报告：环境、36 场景矩阵、HOL、推荐、复现、局限 | 报告 §0 TL;DR、§3 矩阵、§5 推荐 |
| `.github/workflows/ci.yml` | push/PR 流水线：gofmt → build → vet → `go test -race` → xcaddy 构建并校验模块 | `gofmt`、`go build`、`go vet`、`go test -race`、`list-modules` |
| `.github/workflows/release.yml` | tag `v*` 发布：linux/amd64+arm64 构建 + GitHub Release | `xcaddy build`、`softprops/action-gh-release` |

---

## 3. 模块注册与装配

### 3.1 四个 Caddy 模块

| Caddy 模块 ID | 短名（Caddyfile / JSON） | 类型 | 注册点 | 类型符号 |
|---|---|---|---|---|
| `tunnel` | `tunnel`（app） | App | `app.go` `init()` → `caddy.RegisterModule(TunnelApp{})` | `TunnelApp` |
| `http.handlers.tunnel_server` | 指令 `tunnel_server`；JSON `"handler": "tunnel_server"` | HTTP Handler | `server.go` `init()` → `caddy.RegisterModule(TunnelServer{})` | `TunnelServer` |
| `http.handlers.tunnel_status` | 指令 `tunnel_status`；JSON `"handler": "tunnel_status"` | HTTP Handler | `status.go` `init()` → `caddy.RegisterModule(TunnelStatus{})` | `TunnelStatus` |
| `http.reverse_proxy.transport.tunnel` | transport 短名 `tunnel`；JSON `"protocol": "tunnel"` | `http.RoundTripper` | `transport.go` `init()` → `caddy.RegisterModule(TunnelTransport{})` | `TunnelTransport` |

两点容易踩：

- handler 的 JSON 名字是 **去掉 `http.handlers.` 前缀**的短名；transport 在 `reverse_proxy` 里用 `protocol`（或 Caddyfile 的 `transport <短名>`）引用，短名是 `tunnel`。集成测试就是这么写的：`"transport": {"protocol": "tunnel", "node": "node1", "port": "8080"}`。
- 只有两个 handler 需要 `httpcaddyfile.RegisterHandlerDirective`（`server_caddyfile.go` 的 `init()`）；transport **不走** handler directive，它通过 `caddyfile.Unmarshaler`（`TunnelTransport.UnmarshalCaddyfile`）被 `reverse_proxy` 的 `transport` 子指令解析。**这就是"Caddyfile 解析分两个文件"**（§8 第 5 条）。

### 3.2 Provision 顺序与节点合并

Caddy 的 app 是**懒加载 + 复用**的：第一次 `ctx.App("tunnel")` 会创建并 `Provision` `TunnelApp`，之后所有持有者拿到同一实例。因此**无论哪个 handler/transport 先被 provision，`TunnelApp.Provision` 都保证先跑一次**。

```
Caddy 加载配置
   │
   ├─(1) TunnelApp.Provision            app.go
   │        registry = newNodeRegistry(); metrics = newMetrics()
   │        for name,cfg := range Nodes:
   │            cfg.fill() → validateNodeConfig(ports 非空 / channels>0 / max_streams>0 / 数字 id)
   │            registry.put(newNode(name,cfg,heartbeat(),logger))   ← graceWindow = hb×3 在此固化
   │            ▲
   │            │ 由 ctx.App("tunnel") 触发（懒加载，且只跑一次）
   │            │
   ├─(2) TunnelServer.Provision         server.go
   │        app = ctx.App("tunnel")
   │        ① 先写回三件 tunables（>0 才覆盖）：
   │            app.Heartbeat / app.DialTimeout / app.QueueTimeout
   │           （必须在建 node 之前，否则新 heartbeat 不会体现在 graceWindow）
   │        ② 再把本 handler 块的 node 合并进 app：
   │            cfg.fill() → validateNodeConfig(...)
   │            app.Nodes[name] = cfg            ← 覆盖同名 app 级配置
   │            if registry 里没有 name:
   │                registry.put(newNode(name,cfg,hb,h.log))
   │
   ├─(3) TunnelStatus.Provision         status.go     （只取 app 引用）
   └─(4) TunnelTransport.Provision      transport.go
            app = ctx.App("tunnel")
            要求 node/port 非空；port 必须是数字 → portID uint64
            构造 http.Transport{DialContext:dialer.DialContext, DisableKeepAlives:true,
                                ExpectContinueTimeout:1s}   ← 故意不设 ResponseHeaderTimeout
            ★ 不缓存任何超时；超时在每次拨号时从 app 读（与 provision 顺序无关）
   │
   ▼
TunnelApp.Start()  → go healthSweep()  +  go rateSampler()
```

**合并与覆盖的真实语义**（`server.go` `TunnelServer.Provision`）：

- 允许只写 `tunnel_server { node … }` 而不在 app 级重复声明节点 —— 这就是 `test/Caddyfile.test` 的用法（Caddyfile 顶层只有 handler 块，没有显式 `apps.tunnel`）。
- **同一个 node 名不要同时在 app 级 `apps.tunnel.nodes` 和 `tunnel_server` 块里声明**：`app.Nodes[name]` 会被 handler 的配置覆盖，但 registry 里的 `Node` 对象只会在"不存在时"创建，仍持有 app 级那份 `*NodeConfig`。结果是**鉴权读 handler 配置、容量/端口读 app 配置**，形成配置分叉。要么只在 app 级写，要么只在 handler 块写。
- **全局 tunables 有两个入口**：JSON 写在 `apps.tunnel.{heartbeat,dial_timeout,queue_timeout}`；Caddyfile 写在 `tunnel_server` 块顶（三个同名顶层指令）。handler 侧的值 `>0` 时在 (2) 写回 resident app，因此**同一次配置里 Caddyfile 指令优先于 app JSON**。
- **节点会抄走一份超时快照**：`newNode(...)` 后由调用方设置 `Node.dialTimeout` / `Node.queueTimeout`（app 值），**只用于给 client 下发 wire config**（`welcome` / `reload_config`）。实际拨号仍每次从 app 读（§5.5）——两条路径别混，运行期也不要改这两个字段（注释写明 "set once, never mutated"）。
- 节点在 `Provision` 阶段只是"配置对象"，与 client 是否连上无关；`Node.control == nil` 就是"还没人连"。

### 3.3 Start / Stop 生命周期

```
Start()
 ├─ go healthSweep()     周期 = min(heartbeat, 5s)
 │    每 tick: 每个 node → checkGrace() ; metrics.update(node)
 ├─ go rateSampler()     周期 = rateSampleInterval (1s)
 │    每 tick: 每个 node → sampleTraffic()
 └─ lastApp = a          ← 仅供测试取活跃实例

Stop()
 ├─ close(sweepStop) ; <-sweepDone
 ├─ close(rateStop)  ; <-rateDone
 └─ for node in registry.all():
        sendDrain(ctx(2s), 2s)      // 通知 client 停新流、等在途流
        shutdown()                  // 关控制通道 + 关全部数据通道 + 清 session
```

注意 `Stop` 里 drain 的 deadline 是**硬编码 2s**，它只影响发给 client 的 `drain.deadline` 字段，**不会**等 2s 再关；`shutdown()` 紧接着就执行。真正的优雅停机依赖 Caddy 的 process 语义 + client 重连补齐，设计文档 §13 已说明 reload 会断长连接。

### 3.4 Caddyfile 指令的注册与解析

- `server_caddyfile.go` `init()`：
  - `httpcaddyfile.RegisterHandlerDirective("tunnel_server", parseTunnelServerCaddyfile)`
  - `httpcaddyfile.RegisterHandlerDirective("tunnel_status", parseTunnelStatusCaddyfile)`
- `parseTunnelServerCaddyfile`：顶层接受四类 token —— `node <name> { … }` 与三个全局 tunable `heartbeat` / `dial_timeout` / `queue_timeout`；其余 token 报错并提示可用关键字。
  - 三个 duration 用 `time.ParseDuration` 解析，**负值直接拒绝**；结果写进 `TunnelServer` 的同名字段，在 `Provision` 时写回 resident app（§3.2）。
  - node 块解析完立刻 `cfg.fill()` + `validateNodeConfig(...)`，**配置错误在解析期就失败**，不用等 Caddy 启动。
- `parseNodeBlock` 支持的键：`key` / `hmac_secret` / `channels` / `max_streams_per_conn` / `port <id> [addr]`。
  - `channels` / `max_streams_per_conn` 显式写 `<=0` 在**解析期**报错（提示 "omit it to use the default"）；省略则走 `fill()` 的默认值。
  - `port` 的 id 在这里做一次 `ParseUint` 校验，缺省地址补 `127.0.0.1:<id>`。
- `parseTunnelStatusCaddyfile`：只支持 `auth_token <token>`；`tunnel_status` 也可以完全不带块。
- `TunnelTransport.UnmarshalCaddyfile`（`transport.go`）支持两种写法：内联 `transport tunnel node1:8080` 与展开块 `transport tunnel { node … port … }`，内联参数缺 `:` 会报错。**transport 没有承载超时的配置项**——超时一律在 `apps.tunnel` 或 `tunnel_server` 块顶设置。

**两套 0 值语义**（改配置解析时最容易混淆）：

| 入口 | 省略 / `0` | 显式非法值 |
|---|---|---|
| JSON（`apps.tunnel.nodes`） | `fill()` 补默认（`4` / `256`），**合法** | 负数同样被 `fill()` 换成默认（不报错） |
| Caddyfile（`node` 块内 `channels 0`） | 省略 → `fill()` 补默认 | **解析期报错**（`config_test.go` 的 `TestTunnelServerRejectsZeroChannels`） |

---

## 4. 协议与握手

> 契约来源：`modules/tunnel/proto/`。**client 仓库镜像这个包**，改字段名/线格式 = 破坏兼容，必须升 `ProtocolVersion`（§4.5）。

### 4.1 控制通道握手（hello → welcome + config）

控制通道是 client 启动后**第一条** WS，每个 node **恰好一条**。

```
 Client（只持 url + key）                          Caddy: TunnelServer.ServeHTTP
   │                                                   │
   │ 1. WS /ws（建议 wss）                              │ websocket.Accept(CompressionDisabled)
   │──────────────────────────────────────────────────►│ SetReadLimit(-1)          ← 必须！
   │                                                   │
   │ 2. 第一条二进制消息 = envelope{type:"hello"}        │ readHello()
   │    payload: {role:"control", node, key,           │   ├ 必须是 binary
   │              ts, nonce, sig}                      │   └ 第一条必须是 hello
   │──────────────────────────────────────────────────►│
   │                                                   │ 3. AuthenticateHello(hello)
   │                                                   │      ├ hmac_secret 非空 → 验 sig/ts/nonce
   │                                                   │      └ 否则 → 恒定时间比 key
   │                                                   │    失败: 日志 warn，对端只看到
   │                                                   │          close(1008,"auth failed")
   │                                                   │ 4. app.Node(node) → *Node
   │                                                   │
   │ 5. {"type":"welcome","payload":{                  │ attachControl()
   │       session, heartbeat, channels,               │   ├ 复用/铸造 session
   │       config:{ports, max_streams_per_conn,        │   ├ 组装 Welcome{Session,Heartbeat,
   │                channels, dial_timeout,            │   │      Channels, Config, Mux}
   │                queue_timeout},                    │   ├ sendEnvelope(welcome)
   │       mux:{keepalive_interval,                    │   ├ go pingLoop(heartbeat)
   │            keepalive_timeout}}}                   │   └ readLoop(onDrop) ← 阻塞
   │◄──────────────────────────────────────────────────│
   │                                                   │
   │ 6. 之后双向：ping/pong、reload_config、drain        │
   │◄─────────────────────────────────────────────────►│
   │                                                   │
   │ 7. WS 断开 → readLoop 返回 → onDrop:               │
   │     n.controlDown = now（进入宽限期，§7）           │
```

要点：

- **client 只在这里读初始配置**（`welcome.config`），没有独立的 `config` 帧。
- `welcome.heartbeat` 是字符串（如 `"15s"`），取自 `app.heartbeat()`，即配置值；`welcome.channels` 是固定数据通道数，`welcome.session` 是数据通道绑定凭据。
- `welcome.config.dial_timeout` / `queue_timeout` 现在是**真实配置值**：`attachControl` 在 `n.mu` 锁内用 `toProtoConfig(n.dialTimeout, n.queueTimeout)` 构建（拷贝 `Ports`，避免与 `Reconfigure` 的 map 替换竞争），随后随 `welcome` 下发；`reload_config` 走同一出口。
- `welcome.mux` 目前只填 `keepalive_interval` / `keepalive_timeout`（`attach.go` 的 `smuxConfig`：10s / 30s），其余字段（窗口、帧大小）未下发。
- 鉴权失败只把细节写日志（`"tunnel auth failed"`，含 node/role/remote/error），对端一律收到通用的 `"auth failed"`，避免向未认证方泄露"是密钥错还是时钟偏"。

### 4.2 数据通道建立与会话绑定（smux 角色）

数据通道由 client 发起 **固定 `channels` 条**，每条独立 WS。

```
 Client                                             Caddy
   │ 1. WS /ws                                          │
   │     hello{role:"data", node, key,                 │ readHello → AuthenticateHello
   │           session, channel_id, ts,nonce,sig?}     │ app.Node() → *Node
   │───────────────────────────────────────────────────►│ node.attachData()
   │                                                   │   ├ session 必须 == n.session
   │                                                   │   │   否则 close(1008 "invalid or missing session")
   │                                                   │   ├ websocket.NetConn(ctx, conn, MessageBinary)
   │                                                   │   ├ countingConn 包裹           ← 字节计数点在 smux 之下
   │                                                   │   ├ smux.Server(cc, smuxConfig())
   │                                                   │   └ n.channels[channel_id] = ch
   │ 2. welcome{channel_id}                            │
   │◄──────────────────────────────────────────────────│
   │                                                   │
   │ smux.Client(netConn)        ← 同一条 WS →        smux.Server
   │   AcceptStream()                            OpenStream()  ← 由 NodeDialer 调用
```

**角色分工是单向的，不要写反**：

| 侧 | smux 角色 | 谁调用 | 用途 |
|---|---|---|---|
| Caddy（服务端） | `smux.Server(conn, cfg)`（`newDataChannel`） | `dataChannel.session.OpenStream()`（`dialer.go`） | 每个外部 HTTP 请求开一条流 |
| client（agent） | `smux.Client(conn, cfg)`（如 `stubclient`） | `sess.AcceptStream()`（`handleStream`） | 收流 → 读流头 → dial 本地 |

**`attachData` 里的那个 `AcceptStream` 循环是故意的**：服务端永远不该收到入向流，该循环只是"阻塞到会话死亡"的廉价检测手段（smux 没有更直接的 done channel）；万一真有入向流，读完即丢。

**同一 `channel_id` 重连**是合法操作：新通道替换旧的，旧通道被 `close()` + `retireChannelLocked()`，并且 `dataReconnectTotal++`（`/status` 的 `data_reconnect_total` 由此而来）。

**smux keepalive 与心跳是两回事**：数据通道保活由 smux 负责（`KeepAliveInterval=10s`、`KeepAliveTimeout=30s`），`heartbeat` **只**用于控制通道 ping/pong。别拿 `heartbeat` 去调 smux。

### 4.3 流头格式与端口寻址

流头在每条 smux 流的最前面，只用来告诉 client "这条流要去哪个本地端口"：

```
 ┌──────────┬────────────────────────┬─────────────────────────┐
 │ ver (1B) │ port id (uvarint)      │  原始 HTTP/字节流…        │
 │  = 0x01  │  仅数字，1–10 字节      │  （隧道层不再解析）       │
 └──────────┴────────────────────────┴─────────────────────────┘
```

- 写：`proto.WriteStreamHeader(w io.Writer, portID uint64)` —— 一次性写入 `ver` + `binary.PutUvarint`，**必须在把流交给 `http.Transport` 之前写完**（§8 第 1 条）。
- 读：`proto.ReadStreamHeader(r ByteReader) (ver uint8, portID uint64, err error)`，版本不等于 `ProtocolVersion` 返回 `proto.ErrUnsupportedVersion`；`ByteReader` 是 `io.Reader + io.ByteReader` 的最小接口。
- 客户端必须用 `proto.NewHeaderReader(stream)`（`bufio.Reader`）读头，并**继续用同一个 reader** 读后续字节，否则缓冲区里的载荷会丢。
- 例：`8080` 的 uvarint 是 `0x90 0x3F`，所以完整流头是 `01 90 3F`；`stubclient` 的 `handleStream` 就是用 `NewHeaderReader` + `ReadStreamHeader` 取回 `8080` 再查允许清单。

**为什么端口 id 只能是数字**（三处校验，缺一不可）：

1. 线格式：`uvarint` 只能编码无符号整数，`node:port` 寻址因此被约束成 `node:<数字>`。
2. Caddyfile：`parseNodeBlock` 的 `port` 分支用 `strconv.ParseUint` 校验。
3. 配置装载：`app.go` 的 `validatePorts` 对每个 id 再查一次（JSON 配置也可能绕过 Caddyfile）。
4. transport 侧：`TunnelTransport.Provision` 把 `port` 解析成 `portID uint64` 存进 `NodeDialer`。

想支持"名字端口"的话，不是加个 `strings.Contains` 校验就行——要换流头编码（例如长度前缀 + 字节串）并升 `ProtocolVersion`。

### 4.4 proto 包契约：消息类型与结构体

`Envelope` 是唯一的帧格式：**一条 WS 二进制消息 = 一个 JSON envelope**，`payload` 是原样 JSON。

```json
{"type":"hello","payload":{ … }}
```

| `type` 常量 | 方向 | 结构体 | 当前实现 |
|---|---|---|---|
| `TypeHello` (`hello`) | client → Caddy | `Hello` | 必需；`readHello` 强制第一条 |
| `TypeWelcome` (`welcome`) | Caddy → client | `Welcome` | 控制欢迎（含 `Config`）/ 数据欢迎（含 `ChannelID`） |
| `TypeConfig` (`config`) | — | `Config` | **只作为结构体复用**，从不单独成帧 |
| `TypeReloadConfig` (`reload_config`) | Caddy → client | `Config` | `pushReloadConfig` / `PushReloadConfig` 发送（管理面/测试） |
| `TypePing` / `TypePong` | 双向 | `Ping` / `Pong` | `pingLoop` 发、`readLoop` 回；`pong.ts` 算 RTT |
| `TypeStats` (`stats`) | client → Caddy | `Stats` | **已接线**：`control.readLoop` → `cc.onStats` → `Node.applyStats`；驱动 `/status` 的 `client` 块与 `tunnel_local_dial_errors_total`（§10.5） |
| `TypeDrain` (`drain`) | Caddy → client | `Drain` | `Stop` 时 `sendDrain` 发送 |
| `TypeBye` (`bye`) | 双向 | `Bye` | 只处理入向：收到即结束 `readLoop`；服务端不主动发 |
| `TypeError` (`error`) | 双向 | `Error` | **未使用**（服务端目前只用 WS close code 表达拒绝） |

`Hello` 的字段语义：

- 控制通道：`role` / `node` / `key`，HMAC 节点再加 `ts` / `nonce` / `sig`。
- 数据通道：额外必须带 `session`（控制通道 `welcome.session` 原值）与 `channel_id`。
- `role` 只接受 `proto.RoleControl` / `proto.RoleData`；其它当场 `close(1003,"unknown role")`。

### 4.5 协议演进规则

1. 任何**线格式**变化（流头布局、`Envelope` 结构、字段含义）都必须升 `proto.ProtocolVersion`，并在 `ReadStreamHeader` 保留旧版本兼容或明确拒绝策略。
2. 握手消息**没有独立版本协商字段**——`ProtocolVersion` 目前只体现在流头首字节。也就是说：握手成功不代表版本一致，第一条流才会暴露版本不匹配（`ErrUnsupportedVersion` → 该请求 502）。若要更强的失败前置，需要在 `hello`/`welcome` 里加 `proto` 字段（属于协议变更）。
3. 加字段优先用带 `omitempty` 的可选字段（现有 `Hello.Ts/Nonce/Sig`、`Config.*` 都是这种风格），避免旧 client 反序列化失败。
4. 改完记得同步更新 `proto/messages.go` 的注释与本文 §4.4 的表。

---

## 5. 请求全链路

### 5.1 单请求时序

```
外部 HTTP 请求
   │
   ▼
reverse_proxy（Caddy 内置，transport = TunnelTransport）
   │
   ▼
TunnelTransport.RoundTrip                        transport.go
   │  ① URL.Scheme 空 → "http"；URL.Host 空 → Node 名
   │  ② 调 t.rt.RoundTrip(req)（stdlib http.Transport）
   ▼
NodeDialer.DialContext                           dialer.go
   │  每次拨号现场读 app.queueTimeout() / app.dialTimeout()   ← 不在 Provision 快照
   │  ├ node 未注册 ──────────────────────────► 503（metric result=node_down）
   │  └ PickChannel → OpenStream → 限时写流头
   ▼
Node.PickChannel(ctx, queueTimeout)              node.go
   │  ├ 控制通道断（newStreamsAllowed=false）──► 503（result=control_down）
   │  ├ ctx 取消/超时 ────────────────────────► 错误返回（result=client_canceled，请求多已被放弃）
   │  ├ 其它选择失败 ─────────────────────────► 错误返回（result=pick_error）
   │  ├ 全通道满且排队超时 ───────────────────► 503（result=queue_timeout）
   │  └ 抢到槽位（reserve）→ 返回 dataChannel
   ▼
dataChannel.session.OpenStream()                 channel.go
   │  └ 失败 → ch.release() ──────────────────► 502（result=open_stream_error）
   ▼
streamConn{Conn, ch}    ← Close() 时 release()（sync.Once 保证一次）
   │  SetWriteDeadline(now + dial_timeout)  ← dial_timeout 只覆盖这段
   │  ├ WriteStreamHeader 失败 → sc.Close() ──► 502（result=header_error）
   │  └ 写完立即 SetWriteDeadline(零值) 清除
   ▼
返回 net.Conn → http.Transport 在流上写 HTTP/1.1 请求字节（**不受隧道超时约束**）
   │
   ▼
client：AcceptStream → ReadStreamHeader → 查允许清单 → dial 本地 → io.Copy（双向）
   │
   ▼
响应原路返回 → reverse_proxy 回写外部；流关闭 → 预留槽位归还
```

### 5.2 逐段代码定位

| 阶段 | 位置 | 符号 | 关键行为 |
|---|---|---|---|
| 进入 transport | `transport.go` | `TunnelTransport.RoundTrip` | 补 `Scheme`/`Host`；调 `t.rt.RoundTrip`；错误分类 |
| 连接拨号 | `dialer.go` | `NodeDialer.DialContext` | 按名取 node（懒解析）；**每次拨号读** `app.queueTimeout()` / `app.dialTimeout()`；`PickChannel`；`OpenStream`；限时写流头 |
| 选通道 + 背压 | `node.go` | `Node.PickChannel` / `leastLoaded` / `recordPeak` | 最少并发 + CAS 预留；全满则排队到 `queue_timeout` |
| 开流 | `channel.go` | `dataChannel.session` + `streamConn` | smux 开流；Close 时 `release()` 槽位 |
| 写流头 | `proto/streamheader.go` | `WriteStreamHeader` | ver + uvarint；外层由 `stream.SetWriteDeadline` 限时 |
| 开流指标 | `dialer.go` | `streamOpenResult` / `countStreamOpen` | 每次开流给 `tunnel_stream_open_total{result}` +1（结果清单见 §14.4） |
| 回写响应 | stdlib `http.Transport` | `t.rt` | `DisableKeepAlives: true`、`ExpectContinueTimeout: 1s`、**`ResponseHeaderTimeout` 故意为 0**（§5.5） |
| 状态码映射 | `transport.go` | `newStatusResponse` | 503 + `Retry-After: 1`；其余 error → reverse_proxy 的 502 |

### 5.3 错误映射：sentinel → HTTP status

`node.go` 定义了三个 sentinel，但**只有两个真正参与映射**：

| sentinel | 定义位置 | 语义 | 映射 | 备注 |
|---|---|---|---|---|
| `ErrQueueTimeout` | `node.go` | 全通道饱和，`queue_timeout` 内没等到槽位 | **503** | `RoundTrip` 用 `errors.Is` 命中后**合成响应**返回 |
| `ErrNodeDownNewStream` | `node.go` | 控制通道断 / 节点未注册 | **503** | 同上；`dialer` 里对"node 未注册"用 `%w` 包装它 |
| `ErrNoOnlineChannels` | `node.go` | 节点在但零数据通道在线 | 文档写 502 | **当前未被任何代码返回**（实际走 `ErrQueueTimeout` → 503）。清理前请勿依赖此 502 |

其它所有错误（`OpenStream` 失败、写流头失败、`ctx` 取消等）都以 `error` 形式返回给 `reverse_proxy`，最终呈现 **502**。

> 为什么要在 transport 里"合成"503？因为 `reverse_proxy` 把 transport 错误统一渲染成 502/504，没有钩子输出 503。`newStatusResponse` 因此手工构造一个 `*http.Response`（`Content-Type: text/plain`、`Retry-After: 1`）并**以 `nil` error 返回**，让 503 语义能穿透。

### 5.4 `RoundTrip` 里的两个小动作

`transport.go` 的 `RoundTrip` 开头做了两件容易被忽视但必需的事：

1. `req.URL.Scheme == ""` → 置 `"http"`。自定义 transport 走的是 `reverse_proxy` 的"非 TLS 上游"路径，scheme 可能为空，而 stdlib `http.Transport` 必须要有 scheme。
2. `req.URL.Host == ""` → 置 `t.Node`。上游 `dial` 在隧道模式下没有意义，Host 用节点名占位，避免 `http.Transport` 拼出非法请求行。

`reverse_proxy` 侧还需要一个 upstream（集成测试里写的是 `"upstreams": [{"dial": "tunnel"}]`）——它只用于满足 reverse_proxy 的配置校验，实际拨号完全由我们的 `DialContext` 接管。

### 5.5 超时语义（哪个超时管哪一段）

| 参数 | 约束范围 | 实现方式 | 配置入口 |
|---|---|---|---|
| `dial_timeout` | **只在 Caddy 侧**：`OpenStream` 之后写流头的这段时间 | `stream.SetWriteDeadline(now+dial_timeout)`，写完立刻清除（`dialer.go`） | `apps.tunnel.dial_timeout` 或 `tunnel_server` 块顶 |
| `queue_timeout` | 全通道饱和时，请求排队等空槽的时间 | `PickChannel` 内的 `time.NewTimer` | 同上 |
| `heartbeat` | 控制通道 ping 周期；宽限期 = ×3 | `pingLoop` ticker；`graceWindow` 在 `newNode` 时固化 | 同上 |
| （无） | **反向的响应阶段完全不受隧道超时约束** | `http.Transport` 的 `ResponseHeaderTimeout` 故意保持 0 | — |

两点必须记住：

- **`OpenStream` 本身不经网络阻塞**（本地分配 smux stream id），所以把 `dial_timeout` 理解成"开流+写头"没问题，但它实际计时的是**写头**这一步；卡在 `OpenStream` 上的场景（例如 smux 会话已死）会立刻返回错误而非等到超时。
- **后端慢不会被插件掐断**：移除了 `ResponseHeaderTimeout` 就是为了避免"后端只要超过 10s 就 502"。需要响应超时请在后端自身或 Caddy 的其他前置机制里做，不要指望隧道层。

**两条读取路径别混**（拨号 vs 下发）：

| 路径 | 读哪里 | 生效时机 |
|---|---|---|
| 实际拨号（`dialer.go`） | `d.app.queueTimeout()` / `d.app.dialTimeout()` | **每次拨号**现场读 resident app |
| 下发 client（`attach.go`） | `Node.dialTimeout` / `Node.queueTimeout`（建 node 时的 app 快照） | `welcome` / `reload_config` 时组装 wire config |

两者在同一份配置下取值一致，但**改一个不会改另一个**：想让 client 看到新值需要 Caddy reload（重建 app/node），不要试图在运行期修改 `Node` 上的这两个字段。

---

## 6. 容量、背压与峰值统计

### 6.1 容量模型与槽位预留

```
理论并发上限 = channels × max_streams_per_conn
              （示例 4 × 256 = 1024）
```

- `channels` 决定**并行 TCP/WS 条数**（HOL 分摊），`max_streams_per_conn` 决定**单条通道允许的并发流**。
- `max_streams_per_conn` 是**服务端准入上限**（`dataChannel.reserve` 的 CAS 循环），不是 smux 协议限制；client 侧不做此限制。
- 槽位生命周期：`leastLoaded → reserve()` 占位 → `streamConn.Close()` → `release()`。开流失败时 `dialer` 显式 `ch.release()`；`streamConn` 用 `sync.Once` 保证**只释放一次**。
- **实测建议**（`docs/M6-benchmark-report.md` §5）：`channels ≥ 2`，容量取 **≥ 1.5–2 × 预期峰值并发**；高吞吐/大响应优先 **4×256**，省连接用 **2×256**。容量刚好等于并发时会出现排队悬崖（p99 从 ~30ms 涨到 130–260ms）。

### 6.2 `PickChannel` 状态机

```
PickChannel(ctx, queueTimeout)
  │
  ├─ !newStreamsAllowed()  ──────────────────────────────► ErrNodeDownNewStream (503)
  │
  ├─ markQueued(+1)：**仅在"正在等槽位"期间算排队**；起 queueTimeout 定时器
  │
  └─► loop:
        ch := leastLoaded(max)
        │    ├ online 通道按 id 排序（确定性），取 active 最小且 < max
        │    ├ 找到后 CAS reserve()：成功 → 返回 ch；竞争失败 → 返回 nil
        │    └ 返回 nil = "这一刻没抢到"
        │
        ├─ ch != nil ──► **先 markQueued(-1) 出队** ──► recordPeak() ──► return ch
        │                （槽位已计入 active；不出队就会被 active+queued 重复计一次）
        │
        └─ ch == nil（饱和）──► 首次: recordSaturated()（每个请求只 +1）；recordPeak()
                select:
                  ctx.Done        → ctx.Err()
                  queueTimeout    → ErrQueueTimeout (503)
                  backoff 到期    → 2ms→4→8→16→32→50ms（上限），继续 loop
                循环末尾再查一次 !newStreamsAllowed() → ErrNodeDownNewStream (503)
        │
        └─ 退出时若仍处于排队状态（成功路径不会）由 defer 补 -1
```

退避设计的用意：饱和时不空转，但也不让排队请求随着时间无限降低响应性；50ms 上限意味着单个排队请求峰值轮询约 20 次/秒。

### 6.3 `peak_demand` / `channels_needed_at_peak` / `saturated_total` 的更新点

| 指标 | 符号 | 何时更新 | 口径 |
|---|---|---|---|
| `queued` | `Node.markQueued` | 进入 `PickChannel` +1；**抢到槽位时先 -1 再进 `recordPeak`**；失败/取消路径由 `defer` 兜底 -1 | 当前**真正在等槽位**的请求数；≥0 |
| `peak_demand` | `Node.recordPeak`（CAS） | ① 每次抢到通道（此时请求已出队）；② 饱和轮询的每一轮 | `max(历史峰值, 在线通道 active 之和 + 真正在排队的 queued)`；**进程内单调高水位**，只增不减，重载/重启清零（设计文档已同步此措辞）。同一个请求不会既算 active 又算 queued（`TestM3_PeakDemandNoDoubleCount` 守着这条） |
| `channels_needed_at_peak` | `recordPeak` 内 | 仅当 `peakDemand` 被抬高时 | `ceil(peak_demand / max_streams_per_conn)` |
| `saturated_total` | `Node.recordSaturated`（atomic +1） | 每个请求**首次**发现"抢不到通道"时 | 注意：包含 `reserve()` 竞争失败（并非只有真正满载才计） |
| `opened_total` | `Node.incOpened` | `DialContext` 写完流头之后 | 成功开流数（不是 HTTP 请求数） |
| `data_reconnect_total` | `attachData` | 同 `channel_id` 被新通道替换时 | 累计重连次数 |

读口径（`/status`）：`peak_utilization = peak_demand / (channels × max_streams_per_conn)`；`streams.active` 是所有仍注册在 `n.channels` 里的通道 `active` 之和。

### 6.4 `/status` 字段来源对照

| JSON 字段 | 来源 | 说明 |
|---|---|---|
| `node`, `up` | `Node.Name`；`control != nil && controlDown.IsZero()` | `up=false` 不一定已经过去宽限期 |
| `control.connected` | 同 `up` | |
| `control.last_seen` | `controlChannel.lastSeenTime()` | UTC，秒级格式 |
| `control.rtt_ms` | `controlChannel.rtt()` | 由 `pong.ts` 计算 |
| `channels.configured` / `online` | `cfg.Channels` / `dataChannel.online()` 计数 | `online()` = 未关闭且 smux 会话未死 |
| `streams.active` / `opened_total` / `queue_depth` | `sum(active)` / `openedTotal` / `queued` | |
| `capacity.*` | §6.3 各符号 | |
| `traffic.bytes_in/out` | `Node.TrafficTotals()`（§10） | `in` = client→Caddy，`out` = Caddy→client |
| `traffic.rate_in_bps/out_bps` | `Node.Rates()`（§10） | **B/s**（字节/秒），沿用历史字段名 `*_bps` |
| `ports` | `cfg.Ports` 的 key | 无序（map 遍历） |
| `last_error` | `lastError`（`dialer` 三处 `setLastError`） | 截断到 512 字节（`maxErrorLen`） |

两个容易让调用方踩坑的细节：

- `?node=` 过滤无命中时，返回的是 `{"nodes": null}`（切片为 nil），不是 `[]`。
- `/status` 没有方法限制，也未做限流；生产建议只在内网/管理面暴露（`auth_token` 只提供 Bearer 头校验）。

---

## 7. 健康巡检与宽限期

### 7.1 控制通道断链检测

```
controlChannel.readLoop(ctx, onDrop)
   ├ 读失败 / 收到 bye / 连接关闭 → return → onDrop()
   └ onDrop: if n.control == cc { n.controlDown = now }   ← 这个身份判断很关键
             cc.close("read loop ended")
```

为什么 `onDrop` 要判 `n.control == cc`：`attachControl` 在"新控制通道替换旧的"时会先 `old.close()`，旧通道的 `readLoop` 随后返回并触发 `onDrop`；若不判身份，就会**错误地把刚恢复的节点标记成 controlDown**，刚接上的新控制通道会被立刻进入宽限期。

### 7.2 `checkGrace` 状态机

```
              attachControl（control != nil, controlDown = 0）
   ┌───────────────┐
   │      UP       │◄──────────────────────────────────┐
   └───────┬───────┘                                   │
           │ readLoop 返回                             │ 宽限期内控制重连
           │ onDrop: controlDown = now                 │ （inGraceReconnect，session 保留）
           ▼                                           │
   ┌───────────────────┐                               │
   │    IN GRACE       │───────────────────────────────┘
   │ 新流 → 503        │
   │ 存量流继续跑       │
   └─────────┬─────────┘
             │ checkGrace: time.Since(controlDown) ≥ graceWindow
             ▼
   ┌───────────────────┐
   │     EXPIRED       │ 关全部数据通道（在途流报错 → 502）
   └───────────────────┘ session = ""、control = nil；后续 data hello 被 session 校验拒绝
```

`graceWindow` 在 `newNode(name, cfg, hb, log)` 里定为 **`hb × 3`**（`heartbeat × 3`）。`hb` 来自 `TunnelApp.heartbeat()`：优先 `apps.tunnel.heartbeat`，或由 `tunnel_server` 块顶的 `heartbeat` 在 `Provision` 时写回 app；两者都为空则 `DefaultHeartbeat`。**这个值在建 node 时固化**，所以 `TunnelServer.Provision` 必须"先写回 tunables、再建 node"（§3.2），否则新 heartbeat 不会体现在宽限期上。

### 7.3 巡检节奏

`app.go` 的 `healthSweep`：

- ticker 间隔 = `heartbeat`，但**上限 5s**（`if interval > 5s { interval = 5s }`），保证宽限期判定不被过长心跳拖慢。
- 每 tick 对每个 node：`checkGrace()` 然后 `metrics.update(n)`（后者现在会读 `TrafficTotals()` / `Rates()`）。
- 与 `rateSampler`（固定 1s）**相互独立**，速率分辨率不受 `heartbeat` 影响。
- ticker 间隔在 `Start` 时按当时的 `heartbeat()` 算一次；`graceWindow` 在建 node 时固化。两者都不随运行期配置变化——改参数靠 Caddy reload（新 app 实例）生效。

### 7.4 宽限期内重连

`attachControl` 中：

```go
inGraceReconnect := !n.controlDown.IsZero() && n.session != ""
if n.session == "" { n.session = generateSession() }  // 仅首次或宽限期过后
```

- 宽限期内重连**保留 session**，于是"旧数据通道"（当时是用旧 session 校验通过的）继续有效，节点无需重建数据通道即可恢复路由。
- 宽限期过期后 `checkGrace` 清空 session，复用旧 session 的 data hello 会被拒绝（`close(1008, "invalid or missing session")`）。

### 7.5 503 / 502 语义

| 状态 | 语义 | 触发点（代码位置） |
|---|---|---|
| **503** | 容量类，稍后可重试 | `ErrNodeDownNewStream`（控制断/节点未注册）；`ErrQueueTimeout`（全满排队超时）；`RoundTrip` 合成的响应带 `Retry-After: 1` |
| **502** | 后端/隧道故障 | `OpenStream` 失败；写流头失败或超出 `dial_timeout`；client 侧端口不在允许清单；client 本地 dial 失败；宽限期过后在途流被中断 |

一句话记忆：**"现在挤不进去"是 503，"进去了但对面坏了"是 502。**

---

## 8. 常见坑与易错清单

> `proto/streamheader.go` 注释里说的 "dev.md §8 easy-error list" 就是这里。以下每条都是真实踩过或极易踩的点，改动前请对照。

1. **流头必须整段写完再交出去。**
   顺序必须是 `OpenStream → WriteStreamHeader → 交给 http.Transport`。若先让 transport 写请求字节、再补流头，头字节会粘在 HTTP 报文中间，client 侧读出 `port_id` 垃圾后查不到允许清单，表现为**每个请求都 502**。

2. **`SetReadLimit(-1)` 不能删。**
   `coder/websocket` 默认 32KiB 单消息上限，而 smux 的数据帧会超过它。删掉这行 → 大响应/大请求时连接被判违规关闭，看起来像"随机断流"。`server.go` 与 `stubclient` 都显式设置了它。

3. **别拿 `heartbeat` 当数据通道保活用。**
   数据通道只信 smux 的 `KeepAliveInterval/KeepAliveTimeout`（`smuxConfig()`：10s/30s）。`heartbeat` 只管控制通道 ping/pong 与宽限期（`×3`）。混用会让"看起来没断"的通道实际已被 smux 判死。

4. **端口 id 只能数字，现在有四处校验。**
   `parseNodeBlock`（Caddyfile 解析）、`validateNodeConfig → validatePorts`（两条装载路径都会走）、`TunnelTransport.Provision`（transport 参数）。新增入口时漏掉任何一处，都会出现"配置能过、运行时 502"。

5. **Caddyfile 解析分散在两个文件，且 transport 不承载超时。**
   handler 指令在 `server_caddyfile.go`，transport 参数在 `transport.go` 的 `UnmarshalCaddyfile`。加配置项时**两个文件都要看**。超时只能配在 `apps.tunnel` 或 `tunnel_server` 块顶——别指望 `transport tunnel { dial_timeout … }`。

6. **`attachData` 的 `AcceptStream` 不是业务逻辑。**
   它只是"阻塞到会话死亡"的哨兵，服务端永远不消费入向流。别在里面加处理逻辑，也别把它删成 `for {}` 空转。

7. **同一 node 名不要在 app 级与 `tunnel_server` 块里重复声明。**
   否则鉴权用的是 handler 配置、容量/端口用的是 app 配置（§3.2），排查时非常费劲。

8. **通道退役必须"恰好一次"，且持写锁。**
   每个通道离开 `n.channels` 的路径（重连替换、detach、grace 过期、shutdown）都要调一次 `retireChannelLocked`，且都在 `n.mu` 写锁内。漏调 → 累计字节少算；调两次 → 字节翻倍，速率也会跳变。

9. **锁边界要分清：别在 `n.mu.RLock()` 里调用会再取锁的方法，也别在锁外读会被整表替换的 map。**
   `TrafficTotals()` / `Rates()` 内部各自取锁，所以 `/status` 与 `metrics.update` 都是"先取快照字段 → `RUnlock()` → 再取流量"。反过来，`toProtoConfig` 会**拷贝 `Ports`**，而 `Reconfigure` 会整表替换 `n.cfg.Ports`——所以 `attachControl` 的 `wireCfg` 和 `pushReloadConfig` 都必须**在 `n.mu` 内**调用 `toProtoConfig`。改动时保持现状。

10. **要 503 就必须合成响应。**
    `reverse_proxy` 会把所有 transport error 变成 502/504。`RoundTrip` 里对容量类错误用 `newStatusResponse` 返回 `(resp, nil)` 才能得到 503。新增可重试错误时记得加进 `errors.Is` 判断。

11. **`dial_timeout` 只覆盖"写流头"，别把响应超时加回来。**
    `dialer.go` 用 `stream.SetWriteDeadline(now+dial_timeout)` 包住 `WriteStreamHeader`，写完立刻清 deadline。`transport.go` 的 `ResponseHeaderTimeout` **故意为 0**：一旦设上，慢后端会被变成假 502，这正是本插件刻意不做的。要限响应时间请在别处做（§5.5）。

12. **全局 tunables 必须在建 node 之前写回 app。**
    `TunnelServer.Provision` 先写 `app.Heartbeat/DialTimeout/QueueTimeout`，再 `newNode(...)`——因为 `graceWindow = heartbeat × 3` 在 `newNode` 时就固化了。调换顺序会让新 heartbeat 对宽限期不生效（`config_test.go` 覆盖了指令解析，但顺序只能靠 code review 守）。

13. **两套 0 值语义别混。**
    JSON 里 `channels: 0` / 省略 = 用默认值（`fill()` 补齐，不报错）；Caddyfile 里显式 `channels 0` = **解析期报错**。`validateNodeConfig` 在 `fill()` 之后运行，所以它的 `channels>0` / `max_streams>0` 分支对 JSON 的 0/负数不会触发（会被 `fill()` 换成默认）。真正稳定生效的是 **`ports` 非空**与 **`key`/`hmac_secret` 二选一**这两条检查。

14. **`stats` 帧的写法约定。**
    `Node.applyStats` 只在 `LocalDialErrors >= 0` 时覆盖计数，并把整个 `*proto.Stats` 指针存进 `clientStats`（供 `/status` 的 `client` 块）。服务端自己的 `traffic.bytes_*` 才是权威口径，client 上报的 bytes 只作对照；别把两者相加。

15. **指标标签基数与刷新节奏。**
    `tunnel_stream_open_total{result}` 的 result 是固定枚举（`ok` / `node_down` / `queue_timeout` / `control_down` / `client_canceled` / `pick_error` / `open_stream_error` / `header_error`），不要拼动态字符串。Gauge 系指标由 `healthSweep` 刷新（≤5s 滞后），与 `/status` 的实时值可能短暂不一致。

16. **原子读约定别改回去。**
    `peakDemand`、`saturatedTotal`、`clientDialErrors` 都是"atomic 写 + atomic 读"。`metrics.update` 与 `/status` 必须继续用 `atomic.LoadInt64`——混用普通读会引入 `-race` 能抓到的数据竞争（上一轮已修）。

17. **`ErrNoOnlineChannels` 仍是无返回点的 sentinel。**
    注释写"映射 502"只是设计意图，没有任何调用点返回它。真实行为：节点在线但零通道 → `PickChannel` 排队到 `ErrQueueTimeout` → **503**。保留它是为了将来接"无在线通道"的显式判断，别在文档/告警里当成已生效。

18. **集成测试/基准不能 `t.Parallel()`。**
    `caddy.Run`/`caddy.Stop` 是**进程级单例**，并行会互相踩。现有用例与 `bench_test.go` 全部串行，`TestMain` 里还有 goleak 兜底。

19. **`/status?node=<不存在>` 返回 `"nodes": null`。**
    客户端解析时别假设数组非空；聚合脚本要判空。新增的 `client` 块是 `omitempty` 指针，**没收到过 `stats` 帧就不会出现**。

20. **`opened_total` 不是请求数。**
    它只在 `DialContext` 成功写完流头后 `incOpened()`，因此"失败于写头/开流"的请求不计入开流总数，但会计入 `tunnel_stream_open_total` 的失败 result。

21. **Caddyfile 的 `reverse_proxy` 必须给占位 upstream。**
    正确写法：`reverse_proxy tunnel { transport tunnel node1:8080 }`。`tunnel` 只是占位，地址不会被拨号（真实寻址由 transport 的 `node`/`port` 决定），但 Caddy 只有在 upstream 列表非空时才会调用自定义 transport；写成 `reverse_proxy { transport tunnel ... }` 会被判定"没有上游"并**直接 503**（适配出的 JSON 里 `upstreams` 为空），transport 根本不会执行。`test/Caddyfile.test` 与 README 示例已按此修正。

---

## 9. 配置参数与默认值

### 9.1 默认值（符号级）

`node.go` 顶部注释 `design §3.4 / dev.md §9` 指的就是这张表。

| 常量 / 字号 | 值 | 定义位置 | 用途 |
|---|---|---|---|
| `DefaultChannels` | `4` | `node.go` | 未配 `channels` 时的固定数据通道数 |
| `DefaultMaxStreamsPerConn` | `256` | `node.go` | 单通道并发流上限 |
| `DefaultHeartbeat` | `15s` | `node.go` | 控制通道 ping 间隔；宽限 = ×3（可配覆盖） |
| `DefaultDialTimeout` | `10s` | `node.go` | `dial_timeout` 的默认值；**只约束写流头**（§5.5），不再用于 `ResponseHeaderTimeout` |
| `DefaultQueueTimeout` | `5s` | `node.go` | `queue_timeout` 的默认值；背压排队超时 |
| `smuxConfig()` keepalive | `10s` / `30s` | `attach.go` | 数据通道保活（与 `heartbeat` 无关） |
| `ExpectContinueTimeout` | `1s` | `transport.go` | stdlib `http.Transport` 的内部握手等待，与隧道超时无关 |
| `healthSweep` 间隔上限 | `5s` | `app.go` | 宽限期判定节奏 |
| `rateWindowSeconds` | `10` | `rate.go` | 速率滑动窗口样本数（1s/样本） |
| `rateSampleInterval` | `1s` | `rate.go` | 速率采样周期 |
| `hmacSkew` / `proto.HMACClockSkewSeconds` | `60s` / `60` | `auth.go` / `proto/auth.go` | 签名 hello 的最大时钟偏移；也是 nonce TTL |
| `nonceCacheSize` | `4096` | `auth.go` | 全局防重放缓存容量 |
| `proto.MaxNonceLen` | `128` | `proto/auth.go` | `hello.nonce` 最大字节数 |
| `proto.ProtocolVersion` | `1` | `proto/version.go` | 流头版本字节 |
| `maxErrorLen` | `512` | `node.go` | `last_error` 截断长度 |
| `Stop` 的 drain deadline | `2s` | `app.go` | 发给 client 的优雅停机期限 |

### 9.2 配置项生效路径

| 配置面 | JSON / Caddyfile | 结构体字段 | 默认填充 | 消费点 | 是否下发 client |
|---|---|---|---|---|---|
| App | `apps.tunnel.nodes` | `TunnelApp.Nodes` | — | `TunnelApp.Provision` | 间接（经 node cfg） |
| App | `apps.tunnel.heartbeat` | `TunnelApp.Heartbeat`（`caddy.Duration`） | `DefaultHeartbeat`（`heartbeat()`） | `attachControl`、`newNode`（graceWindow）、`healthSweep` | `welcome.heartbeat` |
| App | `apps.tunnel.dial_timeout` | `TunnelApp.DialTimeout` | `DefaultDialTimeout`（`dialTimeout()`） | **每次拨号**由 `NodeDialer` 读取，包住写流头（§5.5） | `config.dial_timeout`（经 `Node.dialTimeout` 快照下发，**真实值**） |
| App | `apps.tunnel.queue_timeout` | `TunnelApp.QueueTimeout` | `DefaultQueueTimeout`（`queueTimeout()`） | `NodeDialer` → `PickChannel` 的排队上限 | `config.queue_timeout`（同上，真实值） |
| handler | `tunnel_server { heartbeat / dial_timeout / queue_timeout }` | `TunnelServer.Heartbeat` / `DialTimeout` / `QueueTimeout` | 0 = 不覆盖 | `Provision` 时写回 resident app（早于建 node） | 间接（写回后同上） |
| 节点 | （无配置项） | `Node.dialTimeout` / `Node.queueTimeout` | 建 node 时从 app 抄写 | 仅用于组装 wire config（`toProtoConfig`），不参与拨号 | 是（`config.*_timeout`） |
| 节点 | `key` | `NodeConfig.Key` | 空（**与 hmac_secret 二选一，都空则 provision 报错**） | `AuthenticateHello.authenticatePlaintext` | 否 |
| 节点 | `hmac_secret` | `NodeConfig.HMACSecret` | 空（= 明文模式） | `auth.go` 的 `authenticateHMAC` | 否（**绝不下发**） |
| 节点 | `channels` | `NodeConfig.Channels` | `DefaultChannels` | `attachControl`(`welcome.channels`)、容量计算 | `welcome.channels` + `config.channels` |
| 节点 | `max_streams_per_conn` | `NodeConfig.MaxStreamsPerConn` | `DefaultMaxStreamsPerConn` | `PickChannel`/`leastLoaded` 的 `max` | `config.max_streams_per_conn` |
| 节点 | `ports`（`port` 指令） | `NodeConfig.Ports` | 空 map（**会被 `validateNodeConfig` 拒绝**） | 允许清单，下发后 client 据此 dial | `config.ports` |
| 状态 | `auth_token` | `TunnelStatus.AuthToken` | 空（公开） | `TunnelStatus.ServeHTTP` 的 Bearer 校验 | 否 |
| transport | `node` / `port`（或内联 `node:port`） | `TunnelTransport.Node` / `Port` | 必须显式给 | `Provision` → `NodeDialer`（**不缓存超时**） | 否（`port` 会进流头） |

`NodeConfig.fill()` 是**节点字段**的唯一默认值入口（`app.Provision`、`TunnelServer.Provision`、Caddyfile 解析三处都会调用）；三个全局 tunable 的默认值则在 `heartbeat()` / `dialTimeout()` / `queueTimeout()` 三个访问器里兜底。加新字段时别把默认值散落到第四处。

**鉴权强约束**：`validateNodeConfig` 要求 `key` 与 `hmac_secret` **至少配一个**，两者都空会在 provision 期直接报错（否则"空 key == 空 key"等于无鉴权）。这条与 `fill()` 无关，是真实生效的启动期校验（`TestTunnelServerRejectsUnauthenticatedNode`）。

### 9.3 README / 设计文档与代码的对齐状态

README 已在最后一轮同步中对齐（超时、鉴权、速率、M6、目录、CI 全部更新），**当前没有需要跟进的 README 差异**。

设计文档也已同步关键措辞（`peak_demand` 改为"进程内单调高水位"，M1–M6 状态行指向实测报告）。仍有一处**意图级**差异，不影响使用：

| 位置 | 差异 |
|---|---|
| `design` §8.4 的示例代码 | 示例在 `Provision` 时解析 node、启动期报 `unknown tunnel node`；实现是**拨号时懒解析**（未注册 node → 503），因为 client 通常晚于 provision 才连上、跨 server 的 provision 顺序也无保证。设计文档已在该代码块下注明"以设计意图为准，落地时对照 Caddy v2.11.x" |

### 9.4 下发给 client 的配置

`NodeConfig.toProtoConfig(dialTimeout, queueTimeout)` 是唯一出口，`welcome.config` 与 `reload_config` 复用同一个 `proto.Config`。两个 timeout 由调用方（`attachControl` / `pushReloadConfig`）传入 `Node` 上的快照值，并且**必须在 `n.mu` 内调用**（它会拷贝 `Ports`，与 `Reconfigure` 的整表替换竞争）：

```jsonc
{
  "ports": { "8080": "127.0.0.1:8080" },  // 允许清单：数字 id → 本地 host:port
  "max_streams_per_conn": 256,
  "channels": 4,
  "dial_timeout": "8s",                   // 真实配置值（此处示例配置为 8s，默认 10s）
  "queue_timeout": "3s"                   // 真实配置值（此处示例配置为 3s，默认 5s）
}
```

`welcome` 本身还带 `session` / `heartbeat` / `channels` / `mux`。**注意 `hmac_secret`、`key` 永远不下发。**

---

## 10. 字节计数与速率

### 10.1 计数点在 smux 之下

```
   HTTP 字节
      │
   smux stream            ← 一个 HTTP 请求 一条流
      │
   smux.Session           ← 多路复用器（帧/窗口/keepalive）
      │
   countingConn           ← ★ 计数：Read=bytes_in，Write=bytes_out
      │
   websocket.NetConn      ← WS 二进制消息当字节管道
      │
   WS / TCP / TLS
```

- `countingConn`（`channel.go`）包在 **smux 之下**，因此统计的是**链路字节**（含 smux 帧头、控制帧、keepalive 开销），不是业务 payload。想统计业务量请另找位置（例如 `streamConn` 之上）。
- 口径：`bytes_in` = client → Caddy（我们读），`bytes_out` = Caddy → client（我们写）。
- 每个 `dataChannel` 各自持 `bytesIn/bytesOut`（atomic），`Node` 不重复保存实时值。

### 10.2 `retiredIn` / `retiredOut` 为什么存在

数据通道会被替换/关闭（重连、detach、宽限期过期、shutdown）。如果累计值只读"当前活着的通道"，那么通道一换，**总量会突然变小、速率会突然为负**。所以：

- 每个通道**离开** `n.channels` 时，`retireChannelLocked(ch)` 把它的累计值折进 `n.retiredIn/Out`（必须持写锁、恰好一次）。
- 读总量：`Node.TrafficTotals() = retired* + Σ(活通道)`（持读锁）。
- `rateMeter.observeAt` 里对 `delta < 0` 有兜底：返回 0 速率而不是负数（注释提到"通道退役后短暂"的竞态）。

### 10.3 速率：10 × 1s 滑动窗口，单位 **B/s**

```
rateMeter（每个 node 两个：rateIn / rateOut）
  samples[10]  ← 1 秒 1 个累计值样本（环形）
  observe(total):
     base = 最旧样本（环未满时 samples[0]，满后 samples[next]）
     rate = (total - base.total) / (now - base.at).Seconds()   // 字节/秒
```

- `rateWindowSeconds = 10`、`rateSampleInterval = 1s`：窗口约 10 秒填满；停止流量后约 10 秒内衰减到 0。
- **单位是字节/秒（B/s），不是比特/秒**。字段名 `rate_in_bps` / `rate_out_bps` 是历史命名，与 `bytes_in/out` 同单位，换算 Mbit/s 请自行 `×8 / 1e6`。
- `Node.Rates()` 返回最近一次估算值（不重新采样）；`Node.sampleTraffic()` 才是采样入口，由 `app.rateSampler` 每秒调用。
- Prometheus 侧由 `metrics.update` 写成 `tunnel_bytes_rate_bps{node,dir}`（**B/s**，help 里已注明）；`/status` 侧是 `rate_in_bps` / `rate_out_bps`。

### 10.4 生命周期表

| 时刻 | 动作 | 位置 |
|---|---|---|
| 进程启动 | 每 node 建两个 `rateMeter` | `newNode`（`rateIn/rateOut`） |
| App.Start | `go rateSampler()`，1s tick | `app.go` |
| 每秒 | 每 node `sampleTraffic()` → `TrafficTotals()` → `observe()` | `rate.go` |
| 每次通道离开 | `retireChannelLocked` 折账 | `attach.go` / `rate.go` |
| 每次 WS 读写 | `countingConn` 原子累加 | `channel.go` |
| `/status` 请求 | `TrafficTotals()` + `Rates()`（不采样，只读） | `status.go` |
| App.Stop | 关 `rateStop`、等 `rateDone` | `app.go` |

### 10.5 client 上报的遥测（`stats` 帧）

**链路**（这是 `stats` 从"忽略"变成"有消费方"的关键路径）：

```
client --WS binary--> controlChannel.readLoop   control.go
                        case proto.TypeStats:
                          json.Unmarshal → st
                          c.onStats(&st)        ← attach.go 里注册为 n.applyStats
                              ▼
                        Node.applyStats         node.go
                          ├ LocalDialErrors >= 0 → atomic.Store(clientDialErrors)
                          └ clientStats = &st   （持 n.mu 写锁）
                              ▼
                  /status 的 client 块（clientStats != nil 才出现）
                  + metrics 的 tunnel_local_dial_errors_total
```

口径约定：

- `stats` 是**快照**不是增量：每次覆盖 `clientStats`，client 侧只需报"当前值/累计值"。
- `LocalDialErrors < 0` 视为无效帧，不覆盖计数（`applyStats` 的保护）。
- **服务端 `traffic.bytes_in/out` 才是权威**（计数点在 ws 层，§10.1）；`client.bytes_*` 只用于对照"client 自己看到多少"，两者**不要相加**。
- `client.channels/streams` 是 client 自测值，与 `channels.online` / `streams.active`（服务端视角）可能不一致——排查时以服务端为准，差异本身是信号（例如 client 认为通道在线但服务端已判死）。
- **`test/stubclient` 目前不发 `stats` 帧**，所以仓库内集成测试看不到 `client` 块，`tunnel_local_dial_errors_total` 也保持 0；这条链路的覆盖只有单测 `TestNodeApplyStats`。要用实测验证，需给 stub 加一个周期上报或让真实 client 接入。

---

## 11. 鉴权

### 11.1 两种姿态（按 node 选择，互斥）

| 姿态 | 触发条件 | 凭据 | 校验位置 |
|---|---|---|---|
| 明文 key（legacy） | `hmac_secret` 为空 | `hello.key` | `auth.go` `authenticatePlaintext`（`crypto/subtle` 恒定时间比较） |
| HMAC-SHA256 | `hmac_secret` 非空 | `hello.ts` + `hello.nonce` + `hello.sig` | `auth.go` `authenticateHMAC` |

入口统一为 `TunnelApp.AuthenticateHello(hello *proto.Hello) error`（`server.go` 的 `ServeHTTP` 调用），**控制通道与数据通道的 hello 都要过这一关**。

### 11.2 明文 key 模式

- 比较 `a.Nodes[node].Key == hello.Key`，用 `subtle.ConstantTimeCompare`。
- 旧辅助函数 `TunnelApp.Authenticate(node, key)` 仍保留（兼容旧调用/测试），但它**对 `hmac_secret` 节点永远返回 false**，防止用明文 key 降级绕过签名。
- 明文 key 只应跑在 `wss`（TLS）之上；明文 + `ws` 等于把密钥裸奔。
- ⚠️ **已由配置校验兜底**：`validateNodeConfig` 要求 `key` 与 `hmac_secret` 至少配一个，两者都空会在 provision 期报错——所以不会出现"空 key == 空 key"的隐式放行（`TestTunnelServerRejectsUnauthenticatedNode`）。明文节点生产环境仍建议配合 `wss`。

### 11.3 HMAC 模式（推荐）

**配置**：

```caddyfile
tunnel_server {
    node node1 {
        hmac_secret <secret>   # 一旦设置，key 不再参与鉴权
        channels 4
        port 8080
    }
}
```

```json
{ "apps": { "tunnel": { "nodes": { "node1": { "hmac_secret": "<secret>" } } } } }
```

**签名串**（`proto.CanonicalHello`，client 必须原样镜像）：

```
canonical = node + "|" + <十进制 unix 秒 ts> + "|" + nonce
sig       = hex(HMAC-SHA256(secret, canonical))
```

**校验顺序**（`authenticateHMAC`，廉价检查在前）：

```
nonce 非空且 ≤ proto.MaxNonceLen(128)
  → sig 非空
  → |now - ts| ≤ hmacSkew(60s)          // 注意是秒级整数比较
  → VerifyHelloSignature(...)           // 十六进制解码 + hmac.Equal 恒定时间
  → nonceCache.checkAndStore(key, now)  // 验签通过后才登记，防缓存投毒
```

**防重放**：`nonceCache` 是**全局**（`helloNonces`）、FIFO、容量 `nonceCacheSize = 4096`、TTL = `hmacSkew`（60s）；key 用 `nonceCacheKey(node, nonce)` 做 node 命名空间，避免跨节点误判。满载时淘汰最旧条目——即在极端突发下把重放检测窗口收窄到"最新 4096 个 nonce"，这是有意的**有界内存**权衡。

**失败语义**（`auth.go` 的 sentinel，供日志/测试断言）：

| sentinel | 含义 |
|---|---|
| `ErrUnknownNode` | 节点不存在（含 `nil` hello） |
| `ErrKeyMismatch` | 明文 key 不匹配 |
| `ErrHMACRequired` | 节点要 HMAC，但 hello 没带 sig |
| `ErrHMACInvalid` | 签名不匹配 |
| `ErrHMACStaleTimestamp` | `ts` 超出 ±60s |
| `ErrHMACBadNonce` | nonce 空或超长 |
| `ErrHMACReplay` | nonce 已用过 |

对端只收到通用的 `close(1008, "auth failed")`；具体原因写日志（`server.go` 的 `"tunnel auth failed"`，含 node/role/remote/error），**不向未认证方泄露失败细节**。

**运维提醒**：

- `ts` 是**秒级**且窗口只有 ±60s，服务器与 client 必须做 NTP 同步；时钟漂移是最常见的"HMAC 一直失败"原因。
- 轮换 secret 需要 client 同步换配置（当前没有多密钥/滚动轮换机制）。
- HMAC 只解决"凭据不被明文传输 + 抗重放"，**仍然建议 `wss`**。

### 11.4 数据通道绑定与端口白名单

- **session 绑定**（`attachData`）：data hello 的 `session` 必须等于控制通道 `welcome.session`；否则 `close(1008, "invalid or missing session")`。防止"孤立数据连接注入"。
- **端口白名单**：client 只 dial `config.ports` 里的 id；清单外直接关流，服务端看到流被关 → 该请求 502。服务端权威，client 不自作主张。
- **会话生命周期**：`generateSession()` 用 16 字节 `crypto/rand` → 32 字符 hex；宽限期内重连保留，宽限期后清空。

### 11.5 日志与排查点

| 现象 | 优先看 |
|---|---|
| client 反复被 1008 关闭 | `"tunnel auth failed"` 的 error 字段（区分 size/timestamp/replay） |
| HMAC 全失败但明文正常 | `hmac_secret` 是否配错节点；client 是否用 `CanonicalHello` 生成串 |
| 数据通道全被拒 | 控制握手是否成功（`session` 从哪来）；是否宽限期已过期 |
| 只有某些端口 502 | `config.ports` 是否含该 id；client 的 `LocalAddrOverride`（测试用）是否生效 |

---

## 12. 测试指南

### 12.1 命令

```bash
# 全部单测（含 proto、auth、rate、config、node）
go test ./...

# 竞态检测（CI/合并前必跑）
go test -race ./...

# 只跑集成用例
go test -race -v ./test/

# 只跑某个用例
go test -race -run TestM5_ControlGraceReconnect -v ./test/

# 单元覆盖
go test -cover ./modules/tunnel/...

# M6 压测（固定窗口语义，必须 -benchtime=1x；-run '^$' 跳过普通测试）
go test ./test -run '^$' -bench Benchmark -benchtime=1x

# 冒烟矩阵（-short，约 20s）
pwsh -File scripts/bench.ps1 -Mode smoke

# 完整矩阵（约 4 分钟；M6ENV/M6CSV 打到日志，默认在系统临时目录）
pwsh -File scripts/bench.ps1 -Mode full

# Caddyfile 语法校验（需要先 xcaddy 构建出含插件的 caddy）
./caddy validate --config test/Caddyfile.test --adapter caddyfile

# 与 CI（.github/workflows/ci.yml）等价的本地检查序列
gofmt -l .            # 必须为空
go build ./...
go vet ./...
go test -race -count=1 ./...
# xcaddy 构建后校验 4 个模块已注册
xcaddy build v2.11.4 --with github.com/wjzhangq/caddy-ws-tunnel-plugin=. --output /tmp/caddy-tunnel
/tmp/caddy-tunnel list-modules | grep -E '^(tunnel|http\.handlers\.tunnel_(server|status)|http\.reverse_proxy\.transport\.tunnel)$'
```

> **本机注意**：`go test` 首次运行需要下载模块（Caddy v2.11.4 依赖树很大）。若网络受限，会卡在 `proxy.golang.org` 超时；先配好 `GOPROXY`（如 `goproxy.cn`）/代理并 `go mod download`，或使用已有 module cache 的机器。这属于环境问题，不是代码问题（`scripts/bench.ps1` 会主动检查并提示 GOPROXY）。

### 12.2 测试分层

| 层 | 位置 | 特点 | 是否依赖网络/端口 |
|---|---|---|---|
| 纯单元 | `modules/tunnel/*_test.go`、`modules/tunnel/proto/*_test.go` | 只构造结构体，不起 Caddy | 否 |
| 端到端 | `test/integration_test.go` | `caddy.Run` 起真实进程内 Caddy + stub client + 本地 echo | 是（`127.0.0.1` 随机端口） |
| 基准/压测 | `test/bench_test.go` | 同样进程内 Caddy，但跑固定时长窗口的并发负载，输出 CSV | 是（同上） |

### 12.3 端到端机制（看懂 `integration_test.go`）

1. `TestMain` 用 `goleak.VerifyTestMain` 兜底，忽略 Caddy/certmagic/`net/http` 的**已知长驻协程**（`IgnoreTopFunction`/`IgnoreAnyFunction`）。新增 Caddy 常驻协程导致的 goleak 误报，要往这里加过滤，而不是改业务代码。
2. `freePort()` 先监听 `127.0.0.1:0` 拿端口再关掉。现在额外维护进程内去重表（`freePortsHandedOut` + `freePortsMu`，最多重试 200 次），因为 OS 可能在探测 listener 关闭后把同一个临时端口再发一次，导致 Caddy 报 "listener address repeated"（一次测试里会起两个 server）。
3. `caddyJSON(...)` 拼一份 JSON 配置：`ingress` server 暴露 `/ws*`（`tunnel_server`）与 `/status*`（`tunnel_status`）；`business` server 用 `reverse_proxy` + `transport.protocol = "tunnel"`。`admin.listen` 设为 `127.0.0.1:0` 避免端口冲突。需要自定义节点（如 `hmac_secret`）时用 `caddyJSONWithNode(...)` 直接塞一段 node JSON。
4. `setup()` 起一个**本地 echo HTTP 服务**，再启动 `stubclient.Client{URL, Key:"xx1", LocalAddrOverride: echoAddr}`——`LocalAddrOverride` 把所有端口 id 都指向这台 echo，省得为每个用例准备真实后端；需要慢响应/长流时用 `startSlowEchoServer(...)`。
5. `waitFor()` 轮询等条件成立（最常见是 `client.Channels() == channels`），最多 3s；避免用固定 sleep。
6. `/status` 断言辅助：`statusRates` / `statusBytes` / `statusPeakDemand` / `onlineChannels` / `nodeUp`。新增指标类用例优先复用，别再手写一遍 JSON 解析。
7. 每个用例 `defer cleanup()`：关 client、cancel ctx、停 echo、`caddy.Stop()`。**`caddy.Stop()` 必须调用**，否则下一个用例的 `caddy.Run` 会冲突，goleak 也会报。顺序上先断 client 再 `caddy.Stop()`（`bench_test.go` 的 `m6Env.close` 是范例），避免停机瞬间产生 502 噪声。

### 12.4 `test/stubclient` 的定位

- **非交付件**：真正的 client 在独立仓库，这里只是"能跑通协议的最小桩"。
- 它 mirror 了 `proto` 契约：控制 hello/welcome、N 条 data hello（带 session）、`smux.Client`、`AcceptStream → ReadStreamHeader → 查 allowlist → dial → io.Copy`。
- **支持 HMAC**：`Client.HMACSecret` 非空时，每条 hello 都会生成新 `nonce`（`newNonce()`）并用 `proto.SignHello` 签名（`HMACSecret` 为空则发明文 key）。`TestM5_HMACHelloEndToEnd` 就是靠它覆盖的。
- 便利钩子：
  - `LocalAddrOverride`：把所有端口指向同一后端。
  - `NodeNameForKey`（默认恒为 `"node1"`）：桩的节点名注入点；多节点测试要改它。
  - `CloseControl()` / `ReconnectControl()`：测宽限期重连。
  - `CloseDataChannel(i)` / `ReconnectData(ctx, i)`：测通道补齐。
  - `AllowlistSize()`：测 `reload_config` 是否生效。
  - `Channels()`：已建立的数据通道数。
- **限制**：桩**不发 `stats` 帧**，所以集成测试里 `/status` 的 `client` 块不会出现、`tunnel_local_dial_errors_total` 恒为 0（§10.5）。要覆盖这条链路，给桩加一个周期上报或直接单测 `applyStats`。
- `test/stubclient/json.go` 只是包一层的 `jsonUnmarshal` 薄壳，别以为有特殊逻辑。

### 12.5 用例清单

| 用例 | 覆盖点 |
|---|---|
| `TestM1_SingleStream` | 单流端到端：控制握手 + 1 数据通道 + 流头透传 |
| `TestM2_Concurrent` | 单通道 50 并发流（smux 多路复用） |
| `TestM2_AllowlistMiss` | transport 指向未授权端口 → **502** |
| `TestM3_QueueTimeout503` | `channels=1, max_streams=1`，首个请求占满 → 第二个 **503** |
| `TestM4_Status` | `/status` 与 `?node=` 的字段形状、`up`、通道数、`max_concurrent` |
| `TestM5_DataChannelReAdd` | 杀一条数据通道 → `online` 降 1 → 桩补回 → 恢复 200 |
| `TestM5_ControlGraceReconnect` | 控制断 → 新请求 **503**、数据通道仍在 → 宽限内重连 → 恢复 200 |
| `TestM5_ReloadConfig` | `node.Reconfigure` + `PushReloadConfig` → 桩的允许清单变大（不断连） |
| `TestCheckGraceExpiry` / `TestCheckGraceWithinWindow` | 宽限期过期/未过期的 teardown 语义（单元） |
| `TestPickChannelControlDown503` / `TestPickChannelNoOnlineQueueTimeout` | 选通道的两条 503 分支 + 排队确实等待 |
| `TestPeakDemandTracksQueued` / `TestPeakDemandCeilMath` | 峰值含排队、`channels_needed_at_peak` 的 ceil |
| `TestChannelReserveRelease` | 槽位 CAS 上限 |
| `TestValidatePortsNumericOnly` / `TestNodeConfigDefaults` | 数字 id 校验、默认值 |
| `TestStreamHeaderRoundTrip` / `TestStreamHeaderBadVersion` | 流头边界（0、80、8080、`1<<63-1`）与版本校验 |
| `TestWrapEnvelope` | envelope 编解码 |
| `TestAuthenticatePlaintext*` / `TestAuthenticateHMAC*` / `TestHMAC*` | 明文一致性、签名成功/失败、时钟偏移、重放、nonce 边界、并发、缓存有界 |
| `TestHMACSecretCaddyfileParsing` / `TestHMACSecretJSONConfig` | Caddyfile 与 JSON 两种入口都能设置 `hmac_secret` |
| `TestRateMeter*` / `TestNodeSampleTrafficAndRetire` | 首样本 0、稳态速率、停流衰减、零 elapsed 保护、计数回退保护、退役折账 |
| `TestM3_PeakDemandNoDoubleCount` | 并发抢槽时 `peak_demand` 不把同一请求按 `active + queued` 重复计（回归守卫，§6.3） |
| `TestM4_TrafficRateMetrics` | 持续流量下 `rate_in_bps` / `rate_out_bps` 均 > 0，累计 bytes 不回落 |
| `TestM5_HMACHelloEndToEnd` | HMAC 节点：签名 client 可用 200；同 key 的明文 client 被拒；错 secret 被拒 |
| `TestTunnelServerGlobalDirectives` / `TestTunnelServerGlobalDefaults` | `tunnel_server` 顶层三个 tunable 的解析与"未设置=0=用 app 默认" |
| `TestTunnelServerRejectsInvalidDuration` / `TestTunnelServerRejectsZeroChannels` / `TestTunnelServerRejectsNodeWithoutPorts` | 非法 duration、显式 `channels 0`、空 ports 在**解析期**被拒 |
| `TestValidateNodeConfig` / `TestAppTimeoutDefaultsAndOverrides` / `TestNodeApplyStats` | 校验函数、超时访问器默认/覆盖、`stats` 落库（含 nil/空帧保护） |
| `TestTunnelServerRejectsUnauthenticatedNode` | 既没 `key` 也没 `hmac_secret` 的节点在 provision/解析期被拒（无鉴权节点保护） |
| `TestNodeProtoConfigCarriesTimeouts` | `toProtoConfig(8s, 3s)` 产出的 wire config 带**真实**超时字符串 |
| `BenchmarkTunnelCapacity` / `BenchmarkTunnelHOL` / `BenchmarkEchoDirectBaseline` | M6：容量矩阵、HOL 对比、绕开隧道的直连基线（必须 `-benchtime=1x`） |

### 12.6 加新用例的套路

- 纯逻辑（选通道、峰值、宽限、鉴权、速率、配置解析）→ 放 `modules/tunnel` 包内测试，直接用 `zap.NewNop()` 构造 `newNode(...)`，**不要**起 Caddy；`config_test.go` 的 `parseTunnelServer` / `caddyfile.NewTestDispenser` 是解析类测试的模板。
- 端到端 → 在 `test/` 里用 `setup(...)` 或 `caddyJSONWithNode(...)`，必要时给 `stubclient` 加能力（HMAC 已支持，`stats` 还没有）。
- 涉及时间 → 优先注入可控时钟（`rateMeter.observeAt` 已经支持），不要 `time.Sleep` 等太久；确需真实时间时（如速率窗口）至少留 3–5 个采样周期余量。
- 新增 benchmark → 放 `test/bench_test.go`，遵守"固定窗口 + `-benchtime=1x`"的约定，别用 `b.N` 循环（会与窗口语义冲突，代码里已 `b.Skip` 兜底）。

### 12.7 M6 压测怎么跑

两个入口等价，`scripts/bench.ps1` 只是帮你准备 PATH/GOPROXY、选矩阵并 tee 日志：

```bash
# 直接跑（Windows 下把 go 放进 PATH）
go test ./test -run '^$' -bench 'Benchmark' -benchtime=1x

# 或用脚本（推荐：自动落日志到临时目录）
pwsh -File scripts/bench.ps1 -Mode full       # 约 4 分钟
pwsh -File scripts/bench.ps1 -Mode smoke      # -short 冒烟，约 20s
```

脚本其余可选参数：`-Window` / `-Warmup`（覆盖 `M6_WINDOW` / `M6_WARMUP`）、`-OutFile`（指定日志路径，默认写系统临时目录）、`-GoBin`（本机 Go 安装目录，默认 `%USERPROFILE%\sdk\go1.27.4\bin`）。

**固定窗口语义**：每个场景自己计时（默认 3s，含 300ms 预热），`b.N` 被忽略，所以**必须 `-benchtime=1x`**；否则 benchmark 会 `b.Skip` 并给出提示。

环境变量（逗号分隔，脚本与 `go test` 都认）：

| 变量 | 默认 | 作用 |
|---|---|---|
| `M6_CHANNELS` | `1,2,4` | 容量矩阵的 `channels` 取值 |
| `M6_STREAMS` | `32,128,256` | 矩阵的 `max_streams_per_conn` 取值 |
| `M6_CONC` | `10,50,100,200` | 矩阵的并发 worker 数 |
| `M6_WINDOW` / `M6_WARMUP` | `3s` / `300ms` | 单场景窗口与预热时长 |
| `M6_ECHO_CLOSE` | 未设 | `=1` 让 echo 响应带 `Connection: close` |
| `M6_DEBUG` | 未设 | `=1` 额外打印延迟分布 |

输出约定：

- 第一行 `M6ENV,...` 记录环境（Go/OS/CPU/Caddy/smux/websocket 版本、窗口参数），**写报告时必须带上**，否则数据不可复现。
- 每个场景一行 `M6CSV,...`（表头见 `m6CSVHeader`），含吞吐、p50/p95/p99、错误分类、`/status` 的 `peak_demand` / `channels_needed_at_peak` / `saturated_total` / `opened_total` / `bytes_*`。
- 结果汇入 [`docs/M6-benchmark-report.md`](docs/M6-benchmark-report.md)（**已产出**，含 36 场景原始矩阵、HOL、推荐、复现命令与局限）。

**M6 实测结论摘要**（摘自报告 §0 TL;DR，本机 Ryzen 9 9950X3D / 32 逻辑核 / 全 loopback / 单进程压测台）：

- 512B 响应吞吐 **13.4–17.0k req/s**；并发 10 → 200 吞吐只涨 12–18%，瓶颈在压测台 CPU/隧道栈而非通道数。
- 尾延迟悬崖由**容量是否覆盖并发**决定：并发 200、容量 32/64/128 → p99 **135–259ms**；容量 ≥256 → p99 **26–34ms**。
- `channels` 1→2/4 在小响应下 +5–8%，**64KB 大响应占 50% 时 +87%**（6.9k → 12.9k req/s）。
- 推荐：`channels ≥ 2`，容量 **≥ 1.5–2 × 峰值并发**；高吞吐/大响应用 **4×256**，省连接用 **2×256**。
- 无丢包 loopback 上**未观测到用户态 HOL 放大**（TCP 丢包型 HOL 无法在本机复现，见报告 §7 局限）。

> 仓库卫生：压测会生成 `caddy-tunnel.exe` 等本地构建产物与日志；`.gitignore` 已忽略 `/caddy-tunnel.exe`、`/caddy`、`/dist/`，日志默认写系统临时目录（`scripts/bench.ps1`），提交前仍建议 `git status` 确认。

---

## 13. 扩展指南

### 13.1 加一个节点配置项（以新增 `window` 参数为例，当前不存在，仅示范）

1. **结构体**：`node.go` 的 `NodeConfig` 加字段，带 `json` 标签与 `omitempty`。
2. **默认值**：`NodeConfig.fill()` 里补默认（节点字段的唯一入口，别散落）。若新参数是**全局** tunable，则应放到 `TunnelApp` + `heartbeat()` / `dialTimeout()` / `queueTimeout()` 那类访问器里，并在 `TunnelServer` 上加同名字段以便 Caddyfile 写回。
3. **Caddyfile**：`server_caddyfile.go` 的 `parseNodeBlock` 加 `case "window":`（`NextArg` + 解析 + 错误用 `d.Errf`）；全局参数则加在 `parseTunnelServerCaddyfile` 的顶层 switch。
4. **校验**：非法值要在**配置装载期**失败——把规则加进 `validateNodeConfig`（`app.Provision`、`TunnelServer.Provision`、Caddyfile 解析三条路径都会调用，参照 `validatePorts`），别只加在一个入口。
5. **下发/消费**：需要在 client 侧用 → `toProtoConfig()` 加字段并同步 `proto.Config`；只在服务端用 → 直接读 `n.cfg`。
6. **文档**：README 配置表 + 本文 §9.2 表。
7. **测试**：`node_test.go` / `config_test.go` 加默认值、校验、解析用例（Caddyfile 解析用 `caddyfile.NewTestDispenser`，见 `config_test.go` 的 `parseTunnelServer`）。

### 13.2 加一条控制指令

1. `proto/messages.go`：加新的消息类型常量（与 `TypePing` / `TypeDrain` 同级）与对应结构体。
2. 服务端：发送 → `controlChannel.sendEnvelope(ctx, 常量, payload)`；接收 → `control.go` 的 `readLoop` 加对应 `case`。
3. 若消息要改 `Node` 状态，**不要直接在 `readLoop` 里操作字段**：参照 `stats` 的做法，在 `controlChannel` 上挂一个回调（`onStats` 的模板），由 `Node` 的方法（如 `applyStats`）在自有锁内落库。`readLoop` 里也不要长时间持锁，避免与发送路径互相等待。
4. 客户端：在 client 仓库的等价 readLoop 里实现；`stubclient` 需要的话同步加（仅测试用；注意它目前不发 `stats`）。
5. 更新本文 §4.4 表与设计文档 §5。
6. 若消息带遥测语义，顺手评估是否要进 `/status` 与 Prometheus（`applyStats` → `client` 块 + gauge 是现成模板）。

### 13.3 加一个新 Caddy 模块

```go
func init() { caddy.RegisterModule(MyThing{}) }

func (MyThing) CaddyModule() caddy.ModuleInfo {
    return caddy.ModuleInfo{ID: "http.handlers.my_thing", New: func() caddy.Module { return new(MyThing) }}
}

func (m *MyThing) Provision(ctx caddy.Context) error {
    appIface, err := ctx.App("tunnel")   // 需要 App 就这一句
    if err != nil { return err }
    m.app = appIface.(*TunnelApp)
    return nil
}

// 接口断言（编译期校验）
var _ caddy.Provisioner = (*MyThing)(nil)
```

- 要能被 Caddyfile 用 → 在 `server_caddyfile.go` 的 `init()` 里 `httpcaddyfile.RegisterHandlerDirective`。
- 需要 `Start`/`Stop` → 实现 `caddy.App`；后台协程必须能被 `Stop` 关掉（现有 `healthSweep`/`rateSampler` 是模板）。
- 注册的 Prometheus 指标要带 `node` 标签，且用 `registerOrExisting` 避免 reload 时重复注册 panic。

### 13.4 改协议的检查单

- [ ] `proto` 里改/加字段，保持向后兼容（`omitempty`）。
- [ ] 线格式变化 → 升 `ProtocolVersion`，并在 `ReadStreamHeader` 决定兼容/拒绝。
- [ ] `test/stubclient` 同步（它 mirror 契约）。
- [ ] `streamheader_test.go` / `auth_test.go` 补用例。
- [ ] 设计文档 §4–§6 + 本文 §4 更新。

---

## 14. 故障排查（开发视角）

### 14.1 状态码对照表

| 状态 | 最可能原因 | 定位到符号 |
|---|---|---|
| 502 | 端口不在允许清单 | `config.ports` 与 `TunnelTransport.Port`；client `handleStream` 查表失败 |
| 502 | client 本地端口不可达 | client 侧 dial 失败；client 用 `stats` 帧上报 → `/status` 的 `client.local_dial_errors` 与 `tunnel_local_dial_errors_total`（§10.5） |
| 502 | 写流头失败或超出 `dial_timeout` | `WriteStreamHeader` 返回错误 / `SetWriteDeadline` 触发 → `header_error` |
| 502 | `OpenStream` 失败（smux 会话已死但通道还没清理） | `NodeDialer.DialContext` 的 `open stream:` 分支 → `open_stream_error` |
| 502 | 宽限期过后的在途流 | `checkGrace` 关通道 → 流报错 |
| 503 | 控制通道断（含宽限期内） | `ErrNodeDownNewStream`（`newStreamsAllowed`） |
| 503 | 节点还没注册（client 没连上） | `dialer` 的 `node %q not registered` 包装 |
| 503 | 全通道饱和 + 排队超时 | `ErrQueueTimeout`；看 `/status` 的 `queue_depth` / `saturated_total` |
| 503 持续但 `online=configured` | 单通道打满（least-loaded 失效或 `max_streams_per_conn` 太小） | `leastLoaded` / `reserve` |
| `/status` 打不开 | 路由没挂上 `tunnel_status`，或 `auth_token` 不匹配（401） | `TunnelStatus.ServeHTTP` |

`tunnel_stream_open_total{result}` 与状态码的对应关系（排查时先看这个标签，比 `/status` 更细）：

| result | 含义 | 对外状态 |
|---|---|---|
| `ok` | 开流并成功写完流头 | 正常进入 HTTP 传输 |
| `queue_timeout` | 全通道饱和，排队超时 | 503 |
| `control_down` | 控制通道断（`newStreamsAllowed=false`） | 503 |
| `node_down` | 节点在注册表里不存在（client 从未连上） | 503 |
| `client_canceled` | 请求 ctx 被取消/超时 | 无（调用方已放弃） |
| `pick_error` | 选通道的其它错误 | 通常 502 |
| `open_stream_error` | smux `OpenStream` 失败 | 502 |
| `header_error` | 写流头失败/超时 | 502 |

### 14.2 goleak 报告了泄漏

1. 先看栈顶是不是**已知的 Caddy 长驻协程**（certmagic、admin server、`net/http.persistConn`）→ 在 `TestMain` 加 `Ignore*Function`。
2. 已知历史坑：`caddy.Run()` 时 certmagic 的锁文件维护协程（每 5s 轮询一次）可能活过最后一次 `caddy.Stop()`，让 goleak 误报。**已在 `TestMain` 里忽略**（`goleak.IgnoreTopFunction("github.com/caddyserver/certmagic.keepLockfileFresh")`）；若再遇到同类误报，按同样方式加忽略，**不要**用 `time.Sleep` 去等（不稳定且拖慢测试）。
3. 是**我们自己的**协程（`healthSweep`/`rateSampler`/`pingLoop`/`attachData` 循环）→ 检查是否所有退出路径都 `close(chan)`/`return`。
4. 常见根因：`Stop()` 没被调用（`caddy.Stop` 漏了）；`readLoop`/`connectData` 的 ctx 没被 cancel；smux 会话没关。

### 14.3 关键日志速查

| 日志 | 位置 | 含义 |
|---|---|---|
| `tunnel app provisioned` | `app.Provision` | 节点数 + 生效的 heartbeat/dial_timeout/queue_timeout |
| `tunnel app started` / `tunnel app stopped` | `app.Start/Stop` | 生命周期边界 |
| `tunnel auth failed` | `server.ServeHTTP` | 鉴权失败（细节只在日志里） |
| `control channel reconnected within grace window` | `attachControl` | 宽限期内重连成功，session 保留 |
| `control channel dropped, entering grace window` | `attachControl` onDrop | 开始宽限计时 |
| `grace window expired, node torn down` | `checkGrace` | 清 session、关全部通道（在途流 502） |
| `data channel attached` / `detached` | `attachData` | 通道注册/移除（带 `channel_id`） |

### 14.4 Prometheus 指标

| 指标 | 类型 | 标签 | 说明 |
|---|---|---|---|
| `tunnel_node_up` | Gauge | `node` | 控制通道是否 up |
| `tunnel_channels` | Gauge | `node`, `state` = `configured` / `online` | 通道数 |
| `tunnel_streams_active` | Gauge | `node` | 当前占用槽位 |
| `tunnel_streams_peak` | Gauge | `node` | = `peak_demand` |
| `tunnel_channels_needed_peak` | Gauge | `node` | 峰值时所需通道数 |
| `tunnel_stream_open_total` | Counter | `node`, `result` | **已接线**：`dialer.countStreamOpen` 每次开流 +1；result 见 §14.1 的对照表（固定 8 值） |
| `tunnel_bytes_total` | Gauge | `node`, `dir` = `in` / `out` | **已含退役通道累计**：`metrics.update` 取 `TrafficTotals()`，与 `/status` 同口径，**单调不减**，不会因通道重连回落 |
| `tunnel_bytes_rate_bps` | Gauge | `node`, `dir` = `in` / `out` | 滑动窗口速率，**B/s**（help 已注明不是 bit/s）；取自 `Node.Rates()` |
| `tunnel_node_saturated_total` | Gauge | `node` | 饱和（触发排队）累计次数 |
| `tunnel_local_dial_errors_total` | Gauge | `node` | **已接线**：由 client 的 `stats` 帧驱动（`applyStats` → `clientDialErrors`）；没有 client 上报时保持 0 |

> 刷新节奏：**Gauge 系**由 `healthSweep` 每 tick 刷新（`heartbeat` 上限 5s），所以最多约 5s 滞后，与 `/status` 的实时值可能短暂不一致；**Counter**（`tunnel_stream_open_total`）在开流路径上即时 +1，不滞后。

---

## 15. 里程碑与验收状态

| 里程碑 | 状态 | 交付物 / 验收 | 证据 |
|---|---|---|---|
| M1 单流打通 | ✅ | 控制握手 + 单数据通道 + 流头透传 | `TestM1_SingleStream` |
| M2 多路复用 | ✅ | 单通道多流并发 + 端口允许清单 | `TestM2_Concurrent`、`TestM2_AllowlistMiss` |
| M3 固定连接池 | ✅ | least-loaded + 背压排队 + 峰值统计（含"取槽请求不重复计"回归） | `TestM3_QueueTimeout503`、`TestM3_PeakDemandNoDoubleCount`、`node_test.go` |
| M4 transport + 状态 | ✅ | 内联/块写法 + `/status` + 流量速率指标 | `TestM4_Status`、`TestM4_TrafficRateMetrics`、`TunnelTransport.UnmarshalCaddyfile` |
| M5 健壮性 | ✅ | 宽限重连 + 通道补齐 + reload + drain + metrics + HMAC（端到端） | `TestM5_*`、`TestM5_HMACHelloEndToEnd`、`auth_test.go` |
| M6 压测调参 | ✅ | 可复现基准 + **实测报告**：36 场景矩阵 / HOL / 推荐 / 复现 / 局限 | [`test/bench_test.go`](test/bench_test.go)、[`scripts/bench.ps1`](scripts/bench.ps1)、[`docs/M6-benchmark-report.md`](docs/M6-benchmark-report.md)（512B 13.4–17.0k req/s；容量 ≥256 时 p99 26–34ms；50% 64KB 由 1ch 6.9k → 4ch 12.9k req/s；推荐 4×256 或 2×256、容量 ≥1.5–2× 峰值并发） |

---

## 16. 状态快照与待办

### 16.1 定稿状态与复查命令

见 [§0.3](#03-状态快照定稿)。代码定稿后如需复查，跑一遍 CI 等价序列：

```bash
gofmt -l .                       # 必须无输出
go build ./... && go vet ./...
go test -race -count=1 ./...
grep -rn "dev.md §" modules/     # 确认 §4.3 / §8 / §9 引用仍然成立
grep -rn "TODO\|待实现\|预留" modules/ README.md
```

### 16.2 剩余待办

| 项 | 位置 | 建议 |
|---|---|---|
| `ErrNoOnlineChannels` 仍无返回点 | `node.go` | 保持"保留但未生效"的定位：要么接上（无在线通道→502 的显式判断），要么删掉并改注释；文档/告警别当成已生效 |

> 此前记录的待办均已在最后一轮解决：wire 超时常量 → `toProtoConfig(dialTimeout, queueTimeout)` 下发真实值；无鉴权节点 → `validateNodeConfig` 强制 key/hmac_secret 二选一；构建产物 → `.gitignore`；`README` 目录 → 已补 `config_test.go`；`peak_demand` 措辞 → 设计文档已改；M6 报告 → 已产出。

> 本文由"照着代码写"产生：所有符号名都可在仓库内 grep 到。若发现本文与代码不一致，**以代码为准**并欢迎直接改本文。

---

## 附录 A. 符号索引（符号 → 文件）

> 便于 `grep -n "符号" 文件` 定位；顺序大致按层次。行号未写，避免并行合入时漂移。

| 符号 | 文件 |
|---|---|
| `TunnelApp`, `TunnelApp.Provision`, `Start`, `Stop`, `Node`, `heartbeat`, `dialTimeout`, `queueTimeout`, `healthSweep`, `rateSampler`, `validateNodeConfig`, `validatePorts`, `LastApp` | `modules/tunnel/app.go` |
| `Node`, `NodeConfig`, `fill`, `toProtoConfig`, `newNode`, `onlineChannels`, `newStreamsAllowed`, `PickChannel`, `leastLoaded`, `markQueued`, `recordPeak`, `recordSaturated`, `incOpened`, `setLastError`, `applyStats`, `clientStats`, `clientDialErrors`, `dialTimeout` / `queueTimeout`（Node 字段）, `NodeRegistry`, `newNodeRegistry`, `ErrQueueTimeout`, `ErrNodeDownNewStream`, `ErrNoOnlineChannels`, `Default*` | `modules/tunnel/node.go` |
| `countingConn`, `dataChannel`, `newDataChannel`, `reserve`, `release`, `online`, `activeStreams`, `trafficBytes`, `streamConn` | `modules/tunnel/channel.go` |
| `controlChannel`, `newControlChannel`, `sendEnvelope`, `readLoop`, `pingLoop`, `touch`, `lastSeenTime`, `rtt`, `isClosed`, `onStats` | `modules/tunnel/control.go` |
| `attachControl`, `attachData`, `checkGrace`, `pushReloadConfig`, `PushReloadConfig`, `Reconfigure`, `sendDrain`, `shutdown`, `smuxConfig` | `modules/tunnel/attach.go` |
| `NodeDialer`, `DialContext`, `streamOpenResult`, `countStreamOpen` | `modules/tunnel/dialer.go` |
| `TunnelTransport`, `UnmarshalCaddyfile`, `Provision`, `RoundTrip`, `newStatusResponse` | `modules/tunnel/transport.go` |
| `TunnelServer`, `ServeHTTP`, `readHello`, `Heartbeat`, `DialTimeout`, `QueueTimeout` | `modules/tunnel/server.go` |
| `parseTunnelServerCaddyfile`, `parseNodeBlock`, `parseTunnelStatusCaddyfile` | `modules/tunnel/server_caddyfile.go` |
| `TunnelStatus`, `clientStatus`, `ServeHTTP` | `modules/tunnel/status.go` |
| `metrics`, `newMetrics`, `update`, `bytesRate`, `localDialErrors`, `streamOpenTotal`, `registerOrExisting`, `registerOrExistingCounter` | `modules/tunnel/metrics.go` |
| `AuthenticateHello`, `authenticatePlaintext`, `authenticateHMAC`, `Authenticate`, `hmacSkew`, `nonceCacheSize`, `ErrUnknownNode`, `ErrKeyMismatch`, `ErrHMACRequired`, `ErrHMACInvalid`, `ErrHMACStaleTimestamp`, `ErrHMACBadNonce`, `ErrHMACReplay`, `nonceCache`, `nonceCacheKey`, `helloNonces`, `checkAndStore`, `purgeLocked`, `generateSession` | `modules/tunnel/auth.go` |
| `rateMeter`, `rateSample`, `rateWindowSeconds`, `rateSampleInterval`, `observe`, `observeAt`, `last`, `Rates`, `TrafficTotals`, `retireChannelLocked`, `sampleTraffic` | `modules/tunnel/rate.go` |
| `ProtocolVersion` | `modules/tunnel/proto/version.go` |
| `TypeHello`…`TypeError`, `RoleControl`, `RoleData`, `Envelope`, `WrapEnvelope`, `Hello`, `Welcome`, `Mux`, `Config`, `Ping`, `Pong`, `Stats`, `Drain`, `Bye`, `Error` | `modules/tunnel/proto/messages.go` |
| `WriteStreamHeader`, `ReadStreamHeader`, `NewHeaderReader`, `ByteReader`, `ErrUnsupportedVersion` | `modules/tunnel/proto/streamheader.go` |
| `CanonicalHello`, `SignHello`, `VerifyHelloSignature`, `HMACClockSkewSeconds`, `MaxNonceLen` | `modules/tunnel/proto/auth.go` |
| `TestTunnelServerGlobalDirectives`, `TestTunnelServerRejectsZeroChannels`, `TestTunnelServerRejectsNodeWithoutPorts`, `TestTunnelServerRejectsUnauthenticatedNode`, `TestValidateNodeConfig`, `TestNodeProtoConfigCarriesTimeouts`, `TestAppTimeoutDefaultsAndOverrides`, `TestNodeApplyStats`, `parseTunnelServer` | `modules/tunnel/config_test.go` |
| `TestMain`, `setup`, `caddyJSON`, `caddyJSONWithNode`, `freePort`, `freePortsHandedOut`, `waitFor`, `startSlowEchoServer`, `statusRates`, `statusBytes`, `statusPeakDemand` | `test/integration_test.go` |
| `BenchmarkTunnelCapacity`, `BenchmarkTunnelHOL`, `BenchmarkEchoDirectBaseline`, `m6RunLoad`, `m6EmitEnv`, `m6EmitRow`, `m6LatStats`, `m6CSVHeader` | `test/bench_test.go` |
| `-Mode` / `-OutFile` / `-Window` / `-Warmup` / `-GoBin`（命令行参数名，见 `param()` 块） | `scripts/bench.ps1` |
| `Client`, `Start`, `connectControl`, `writeHello`, `readWelcome`, `controlLoop`, `connectData`, `serveData`, `handleStream`, `Channels`, `AllowlistSize`, `CloseControl`, `CloseDataChannel`, `ReconnectData`, `ReconnectControl`, `Close`, `NodeNameForKey`, `LocalAddrOverride`, `HMACSecret`, `newNonce` | `test/stubclient/stubclient.go` |
