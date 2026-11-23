package test

// M6 压测基准（design §15 M6：HOL 实测 + channels×max_streams_per_conn 配比）。
//
// 本文件是自包含 harness，不修改 integration_test.go 的既有 helper：
//   - 进程内 caddy.Run() 起一套 Caddy：/ws(ingress) + /status + 业务入口(tunnel transport)
//   - stubclient 作为最小隧道 agent，LocalAddrOverride 指向本地 echo HTTP server
//   - 本地 echo server 按路径返回 512B / 64KB / 64KB 慢速分块（可控制响应体大小与延迟）
//   - 固定并发 worker + 固定时长窗口，记录吞吐、p50/p95/p99、错误分类，并从 /status
//     读取 peak_demand / channels_needed_at_peak / saturated_total
//
// 用法（固定窗口语义，必须 -benchtime=1x）：
//
//	go test ./test -run '^$' -bench 'Benchmark' -benchtime=1x
//
// 子集/调参用环境变量（逗号分隔）：
//
//	M6_CHANNELS=1,2,4   M6_STREAMS=32,128,256   M6_CONC=10,50,100,200
//	M6_WINDOW=3s        M6_WARMUP=300ms         M6_ECHO_CLOSE=1（echo 响应带 Connection: close）
//
// 每个场景输出一行 `M6CSV,...`（表头见 m6CSVHeader），便于报告脚本/人工汇总。

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	_ "github.com/caddyserver/caddy/v2/modules/standard"
	"github.com/wjzhangq/caddy-ws-tunnel-plugin/test/stubclient"
)

const (
	// m6PortID 是唯一配置的端口 id；client 端 LocalAddrOverride 会把它映射到 echo。
	m6PortID = "8080"

	m6PathSmall   = "/small"
	m6PathBig     = "/big"
	m6PathTrickle = "/trickle"

	m6SmallBytes   = 512
	m6BigBytes     = 64 << 10 // 64KB
	m6TrickleChunk = 8 << 10  // 慢速大响应分块
	m6TrickleDelay = 5 * time.Millisecond

	// m6Heartbeat 固定 15s（默认值）；单场景窗口 ≪ 心跳，不触发 ping 干扰。
	m6Heartbeat = 15 * time.Second
)

// --- echo server -----------------------------------------------------------

// m6EchoHandler 返回三条路由：小响应(512B)、大响应(64KB)、慢速分块大响应。
// 慢速路由用于制造"长寿命大流"，观察同一 smux 会话内其它短请求是否被拖慢。
func m6EchoHandler() http.Handler {
	small := bytes.Repeat([]byte("S"), m6SmallBytes)
	big := bytes.Repeat([]byte("B"), m6BigBytes)
	closeAfter := os.Getenv("M6_ECHO_CLOSE") == "1"

	writeFixed := func(w http.ResponseWriter, body []byte) {
		if closeAfter {
			w.Header().Set("Connection", "close")
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}

	mux := http.NewServeMux()
	mux.HandleFunc(m6PathSmall, func(w http.ResponseWriter, _ *http.Request) {
		writeFixed(w, small)
	})
	mux.HandleFunc(m6PathBig, func(w http.ResponseWriter, _ *http.Request) {
		writeFixed(w, big)
	})
	mux.HandleFunc(m6PathTrickle, func(w http.ResponseWriter, _ *http.Request) {
		if closeAfter {
			w.Header().Set("Connection", "close")
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(big)))
		fl, _ := w.(http.Flusher)
		for off := 0; off < len(big); off += m6TrickleChunk {
			end := off + m6TrickleChunk
			if end > len(big) {
				end = len(big)
			}
			if _, err := w.Write(big[off:end]); err != nil {
				return
			}
			if fl != nil {
				fl.Flush()
			}
			time.Sleep(m6TrickleDelay) // 块间延迟，让该流在链路上存活 ~40ms
		}
	})
	return mux
}

// m6StartEcho 启动本地 echo HTTP server，返回 host:port。
func m6StartEcho(tb testing.TB) string {
	tb.Helper()
	srv := httptest.NewServer(m6EchoHandler())
	tb.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

// --- Caddy config / env boot ----------------------------------------------

// m6CaddyJSON 构造 Caddy JSON：ingress 提供 /ws、/status；business 走 tunnel transport。
// 全部只监听 127.0.0.1，避免 Windows 防火墙弹窗与外部可达。
func m6CaddyJSON(wsPort, proxyPort, channels, maxStreams int, echoAddr string) string {
	return fmt.Sprintf(`{
  "admin": {"disabled": false, "listen": "127.0.0.1:0"},
  "logging": {"logs": {"default": {"level": "ERROR"}}},
  "apps": {
    "tunnel": {
      "heartbeat": %d,
      "nodes": {
        "node1": {"key": "xx1", "channels": %d, "max_streams_per_conn": %d, "ports": {%q: %q}}
      }
    },
    "http": {
      "servers": {
        "ingress": {
          "listen": ["127.0.0.1:%d"],
          "routes": [
            {"match": [{"path": ["/ws*"]}], "handle": [{"handler": "tunnel_server"}]},
            {"match": [{"path": ["/status*"]}], "handle": [{"handler": "tunnel_status"}]}
          ]
        },
        "business": {
          "listen": ["127.0.0.1:%d"],
          "routes": [
            {"handle": [{"handler": "reverse_proxy",
              "transport": {"protocol": "tunnel", "node": "node1", "port": %q},
              "upstreams": [{"dial": "tunnel"}]}]}
          ]
        }
      }
    }
  }
}`, int(m6Heartbeat), channels, maxStreams, m6PortID, echoAddr, wsPort, proxyPort, m6PortID)
}

func m6FreePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

// m6Env 是一次场景所需的全部运行时对象（Caddy + stubclient + 压测 HTTP client）。
type m6Env struct {
	proxyURL   string
	statusURL  string
	channels   int
	maxStreams int

	client    *stubclient.Client
	cancel    context.CancelFunc
	load      *http.Client
	transport *http.Transport
}

// m6StartEnv 起一套独立 Caddy（每次新端口、独立 config）+ stubclient，并等待
// 数据通道在服务端上线。重复 caddy.Run/Stop 的失败用重试兜底（Windows 端口竞争）。
func m6StartEnv(tb testing.TB, channels, maxStreams int, echoAddr string) *m6Env {
	tb.Helper()

	wsPort, err := m6FreePort()
	if err != nil {
		tb.Fatalf("alloc ws port: %v", err)
	}
	proxyPort, err := m6FreePort()
	if err != nil {
		tb.Fatalf("alloc proxy port: %v", err)
	}

	var cc caddy.Config
	if err := json.Unmarshal([]byte(m6CaddyJSON(wsPort, proxyPort, channels, maxStreams, echoAddr)), &cc); err != nil {
		tb.Fatalf("unmarshal caddy config: %v", err)
	}

	var runErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if runErr = caddy.Run(&cc); runErr == nil {
			break
		}
		tb.Logf("caddy.Run attempt %d/3 failed: %v", attempt, runErr)
		_ = caddy.Stop()
		time.Sleep(200 * time.Millisecond)
	}
	if runErr != nil {
		tb.Fatalf("caddy.Run: %v", runErr)
	}

	env := &m6Env{
		proxyURL:   fmt.Sprintf("http://127.0.0.1:%d", proxyPort),
		statusURL:  fmt.Sprintf("http://127.0.0.1:%d/status?node=node1", wsPort),
		channels:   channels,
		maxStreams: maxStreams,
	}
	env.client = &stubclient.Client{
		URL:               fmt.Sprintf("ws://127.0.0.1:%d/ws", wsPort),
		Key:               "xx1",
		LocalAddrOverride: echoAddr,
	}
	ctx, cancel := context.WithCancel(context.Background())
	env.cancel = cancel
	if err := env.client.Start(ctx); err != nil {
		cancel()
		_ = caddy.Stop()
		tb.Fatalf("stub client start: %v", err)
	}
	m6WaitFor(tb, 8*time.Second, "data channels registered client-side", func() bool {
		return env.client.Channels() == channels
	})
	m6WaitFor(tb, 8*time.Second, "data channels online server-side", func() bool {
		st, err := m6FetchStatus(env.statusURL)
		return err == nil && st.Channels.Online == channels
	})

	env.transport = &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 1024,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
		ForceAttemptHTTP2:   false,
	}
	env.load = &http.Client{Transport: env.transport, Timeout: 15 * time.Second}

	tb.Cleanup(env.close)
	return env
}

// close 停掉压测 client 的空闲连接、断开 stubclient、停 Caddy。
// 顺序很重要：先断 client（避免 Stop 时仍有在途请求 → 502 噪声）。
func (e *m6Env) close() {
	if e.transport != nil {
		e.transport.CloseIdleConnections()
	}
	if e.client != nil {
		e.client.Close()
	}
	if e.cancel != nil {
		e.cancel()
	}
	_ = caddy.Stop()
	time.Sleep(20 * time.Millisecond)
}

// --- /status 快照 ----------------------------------------------------------

// m6NodeStatus 是 /status 响应中与容量/峰值统计相关的子集（design §12）。
type m6NodeStatus struct {
	Node     string `json:"node"`
	Up       bool   `json:"up"`
	Channels struct {
		Configured int `json:"configured"`
		Online     int `json:"online"`
	} `json:"channels"`
	Streams struct {
		Active      int64 `json:"active"`
		OpenedTotal int64 `json:"opened_total"`
		QueueDepth  int64 `json:"queue_depth"`
	} `json:"streams"`
	Capacity struct {
		MaxStreamsPerConn    int     `json:"max_streams_per_conn"`
		MaxConcurrent        int     `json:"max_concurrent"`
		PeakDemand           int64   `json:"peak_demand"`
		ChannelsNeededAtPeak int     `json:"channels_needed_at_peak"`
		PeakUtilization      float64 `json:"peak_utilization"`
		SaturatedTotal       int64   `json:"saturated_total"`
		DataReconnectTotal   int64   `json:"data_reconnect_total"`
	} `json:"capacity"`
	Traffic struct {
		BytesIn    int64 `json:"bytes_in"`
		BytesOut   int64 `json:"bytes_out"`
		RateInBps  int64 `json:"rate_in_bps"`
		RateOutBps int64 `json:"rate_out_bps"`
	} `json:"traffic"`
	Ports     []string `json:"ports"`
	LastError *string  `json:"last_error"`
}

var m6StatusClient = &http.Client{Timeout: 5 * time.Second}

func m6FetchStatus(statusURL string) (m6NodeStatus, error) {
	var out struct {
		Nodes []m6NodeStatus `json:"nodes"`
	}
	resp, err := m6StatusClient.Get(statusURL)
	if err != nil {
		return m6NodeStatus{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return m6NodeStatus{}, fmt.Errorf("/status returned %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return m6NodeStatus{}, err
	}
	if len(out.Nodes) == 0 {
		return m6NodeStatus{}, fmt.Errorf("/status has no nodes")
	}
	return out.Nodes[0], nil
}

// m6StatusAfterDrain 等到活跃流归零后读取 /status，避免把在途流算进快照；
// 等待超时则返回最后一次快照（并在行里记录 streams_active）。
func m6StatusAfterDrain(tb testing.TB, statusURL string) (m6NodeStatus, error) {
	tb.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var last m6NodeStatus
	var lastErr error
	for time.Now().Before(deadline) {
		st, err := m6FetchStatus(statusURL)
		if err == nil {
			last, lastErr = st, nil
			if st.Streams.Active == 0 {
				return st, nil
			}
		} else {
			lastErr = err
		}
		time.Sleep(10 * time.Millisecond)
	}
	return last, lastErr
}

// --- 负载生成 --------------------------------------------------------------

// m6LoadCfg 描述一个压测窗口。
type m6LoadCfg struct {
	Workers     int           // 并发 worker 数
	Duration    time.Duration // 窗口时长
	BigEvery    int           // 普通 worker 每第 N 个请求打 64KB 大响应（0 = 全部小响应）
	SlowWorkers int           // 末尾 N 个 worker 只打慢速 64KB（trickle）
	PathSizes   map[string]int
}

// m6LoadStats 是单个 worker 的统计；窗口结束后合并，避免共享锁成为瓶颈。
type m6LoadStats struct {
	Started  int
	OK       int
	Short    int // 200 但响应体长度不对（流被截断的哨兵）
	HTTP502  int
	HTTP503  int
	HTTPElse int
	NetErr   int
	// NetErrSample 记录第一条网络错误，便于在报告里归因。
	NetErrSample string
	Bytes        int64

	LatSmall []time.Duration
	LatBig   []time.Duration
}

func (s *m6LoadStats) merge(o *m6LoadStats) {
	s.Started += o.Started
	s.OK += o.OK
	s.Short += o.Short
	s.HTTP502 += o.HTTP502
	s.HTTP503 += o.HTTP503
	s.HTTPElse += o.HTTPElse
	s.NetErr += o.NetErr
	if s.NetErrSample == "" {
		s.NetErrSample = o.NetErrSample
	}
	s.Bytes += o.Bytes
	s.LatSmall = append(s.LatSmall, o.LatSmall...)
	s.LatBig = append(s.LatBig, o.LatBig...)
}

// m6LoadResult 是一个窗口的结果。
type m6LoadResult struct {
	Elapsed time.Duration
	Stats   m6LoadStats
}

// m6RunLoad 以固定并发跑满 Duration：每个 worker 串行发 GET，直到墙钟截止。
// 吞吐按"窗口起点→所有请求完成"的真实耗时折算（含少量收尾/排空时间，报告里注明）。
func m6RunLoad(tb testing.TB, client *http.Client, baseURL string, cfg m6LoadCfg) m6LoadResult {
	tb.Helper()
	if cfg.Workers <= 0 {
		tb.Fatalf("m6RunLoad: workers must be > 0, got %d", cfg.Workers)
	}
	if cfg.Duration <= 0 {
		tb.Fatalf("m6RunLoad: duration must be > 0")
	}

	deadline := time.Now().Add(cfg.Duration)
	per := make([]m6LoadStats, cfg.Workers)
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st := &per[i]
			slow := cfg.SlowWorkers > 0 && i >= cfg.Workers-cfg.SlowWorkers
			n := 0
			for time.Now().Before(deadline) {
				path := m6PathSmall
				if slow {
					path = m6PathTrickle
				} else if cfg.BigEvery > 0 {
					n++
					if n%cfg.BigEvery == 0 {
						path = m6PathBig
					}
				}

				req, err := http.NewRequest(http.MethodGet, baseURL+path, nil)
				if err != nil {
					st.NetErr++
					continue
				}
				st.Started++
				t0 := time.Now()
				resp, err := client.Do(req)
				d := time.Since(t0)
				if err != nil {
					st.NetErr++
					if st.NetErrSample == "" {
						st.NetErrSample = err.Error()
					}
					continue
				}
				bodyBytes, copyErr := io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				code := resp.StatusCode
				want := cfg.PathSizes[path]

				switch {
				case code == http.StatusOK && (copyErr != nil || int(bodyBytes) != want):
					st.Short++ // 200 但长度不符 / 读体出错
				case code == http.StatusOK:
					st.OK++
					st.Bytes += bodyBytes
					if path == m6PathSmall {
						st.LatSmall = append(st.LatSmall, d)
					} else {
						st.LatBig = append(st.LatBig, d)
					}
				case code == http.StatusBadGateway:
					st.HTTP502++
				case code == http.StatusServiceUnavailable:
					st.HTTP503++
				default:
					st.HTTPElse++
				}
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	var merged m6LoadStats
	for i := range per {
		merged.merge(&per[i])
	}
	return m6LoadResult{Elapsed: elapsed, Stats: merged}
}

// --- 统计/输出 -------------------------------------------------------------

// m6Quantiles 返回 p50/p95/p99（按样本最近秩：index = floor(q*(n-1))）。
// 传入切片会被就地排序。
func m6Quantiles(lat []time.Duration) (p50, p95, p99 time.Duration) {
	n := len(lat)
	if n == 0 {
		return 0, 0, 0
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	at := func(q float64) time.Duration {
		idx := int(q * float64(n-1))
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		return lat[idx]
	}
	return at(0.50), at(0.95), at(0.99)
}

func m6Ms(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e6 }

// m6Row 是一行可汇总的观测结果。
type m6Row struct {
	Bench       string
	Label       string
	Channels    int
	MaxStreams  int
	MaxConc     int
	Concurrency int
	Mix         string
	Window      time.Duration

	Elapsed        time.Duration
	Stats          m6LoadStats
	P50, P95, P99  time.Duration
	LatN           int
	LatMean        time.Duration
	LatMin, LatMax time.Duration
	// MeanEst 是 Little 定律派生均值：闭流固定并发下 平均时延 ≈ 并发/吞吐（毫秒）。
	// 本机时钟粒度约 0.5ms，亚毫秒的逐请求分位数不可信，但该派生值不受影响。
	MeanEst        float64
	BigN           int
	BigP50, BigP99 time.Duration
	Status         m6NodeStatus
}

var (
	m6HeaderOnce sync.Once
	m6EnvOnce    sync.Once
)

const m6CSVHeader = "M6CSV,bench,label,channels,max_streams,max_concurrent,concurrency,mix,window_s,elapsed_s," +
	"started,ok,short,http502,http503,other,net_err,rps,p50_ms,p95_ms,p99_ms," +
	"lat_n,lat_mean_ms,lat_min_ms,lat_max_ms,mean_est_ms,big_n,big_p50_ms,big_p99_ms," +
	"peak_demand,channels_needed_at_peak,peak_utilization,saturated_total,opened_total,bytes_in,bytes_out," +
	"streams_active,last_error,net_err_sample"

func m6EmitEnv(window, warm time.Duration) {
	m6EnvOnce.Do(func() {
		depVer := func(path string) string {
			if info, ok := debug.ReadBuildInfo(); ok {
				for _, d := range info.Deps {
					if d.Path == path {
						return d.Version
					}
				}
			}
			return "unknown"
		}
		fmt.Printf("M6ENV,go=%s,os=%s/%s,cpu=%d,gomaxprocs=%d,caddy=%s,smux=%s,websocket=%s,window_ms=%d,warmup_ms=%d\n",
			runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.GOMAXPROCS(0),
			depVer("github.com/caddyserver/caddy/v2"), depVer("github.com/xtaci/smux"),
			depVer("github.com/coder/websocket"), window.Milliseconds(), warm.Milliseconds())
	})
}

func m6Sanitize(s string) string {
	s = strings.ReplaceAll(s, ",", ";")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", "")
	return s
}

func m6EmitRow(r m6Row) {
	m6HeaderOnce.Do(func() { fmt.Println(m6CSVHeader) })

	st := r.Stats
	done := st.OK + st.Short + st.HTTP502 + st.HTTP503 + st.HTTPElse
	rps := 0.0
	if r.Elapsed > 0 {
		rps = float64(done) / r.Elapsed.Seconds()
	}
	lastErr := ""
	if r.Status.LastError != nil {
		lastErr = m6Sanitize(*r.Status.LastError)
	}
	// 闭流固定并发下，Little 定律：平均时延 ≈ 并发 / 吞吐。它只依赖窗口墙钟，
	// 不受本机 ~0.5ms 时钟粒度影响，是亚毫秒场景下最可靠的延迟指标。
	if r.MeanEst == 0 && r.Concurrency > 0 && rps > 0 {
		r.MeanEst = float64(r.Concurrency) / rps * 1000
	}
	fmt.Printf("M6CSV,%s,%s,%d,%d,%d,%d,%s,%.2f,%.3f,%d,%d,%d,%d,%d,%d,%d,%.1f,%.3f,%.3f,%.3f,%d,%.3f,%.3f,%.3f,%.3f,%d,%.3f,%.3f,%d,%d,%.4f,%d,%d,%d,%d,%d,%s,%s\n",
		r.Bench, r.Label, r.Channels, r.MaxStreams, r.MaxConc, r.Concurrency, r.Mix,
		r.Window.Seconds(), r.Elapsed.Seconds(),
		st.Started, st.OK, st.Short, st.HTTP502, st.HTTP503, st.HTTPElse, st.NetErr,
		rps, m6Ms(r.P50), m6Ms(r.P95), m6Ms(r.P99),
		r.LatN, m6Ms(r.LatMean), m6Ms(r.LatMin), m6Ms(r.LatMax), r.MeanEst,
		r.BigN, m6Ms(r.BigP50), m6Ms(r.BigP99),
		r.Status.Capacity.PeakDemand, r.Status.Capacity.ChannelsNeededAtPeak,
		r.Status.Capacity.PeakUtilization, r.Status.Capacity.SaturatedTotal,
		r.Status.Streams.OpenedTotal, r.Status.Traffic.BytesIn, r.Status.Traffic.BytesOut,
		r.Status.Streams.Active, lastErr, m6Sanitize(st.NetErrSample))
}

// m6LatStats 汇总已排序样本：n、均值、min、max。
func m6LatStats(lat []time.Duration) (n int, mean, lo, hi time.Duration) {
	n = len(lat)
	if n == 0 {
		return 0, 0, 0, 0
	}
	lo, hi = lat[0], lat[n-1]
	var sum time.Duration
	for _, d := range lat {
		sum += d
	}
	return n, sum / time.Duration(n), lo, hi
}

// m6EmitResult 汇总一个窗口，打印 CSV 行 + 标准 benchmark 指标。
func m6EmitResult(b *testing.B, bench, label string, channels, maxStreams, conc int, mix string,
	window time.Duration, res m6LoadResult, st m6NodeStatus) {
	b.Helper()

	p50, p95, p99 := m6Quantiles(res.Stats.LatSmall) // 就地排序
	bp50, _, bp99 := m6Quantiles(res.Stats.LatBig)
	latN, mean, lo, hi := m6LatStats(res.Stats.LatSmall)

	row := m6Row{
		Bench: bench, Label: label,
		Channels: channels, MaxStreams: maxStreams, MaxConc: channels * maxStreams,
		Concurrency: conc, Mix: mix, Window: window,
		Elapsed: res.Elapsed, Stats: res.Stats,
		P50: p50, P95: p95, P99: p99,
		LatN: latN, LatMean: mean, LatMin: lo, LatMax: hi,
		BigN: len(res.Stats.LatBig), BigP50: bp50, BigP99: bp99,
		Status: st,
	}
	m6EmitRow(row)

	done := res.Stats.OK + res.Stats.Short + res.Stats.HTTP502 + res.Stats.HTTP503 + res.Stats.HTTPElse
	rps := 0.0
	if res.Elapsed > 0 {
		rps = float64(done) / res.Elapsed.Seconds()
	}
	b.ReportMetric(rps, "req/s")
	b.ReportMetric(m6Ms(p50), "p50ms")
	b.ReportMetric(m6Ms(p95), "p95ms")
	b.ReportMetric(m6Ms(p99), "p99ms")
	if len(res.Stats.LatBig) > 0 {
		b.ReportMetric(m6Ms(bp99), "big-p99ms")
	}

	if res.Stats.NetErr > 0 {
		b.Logf("%s: net errors=%d (可能触达本机 loopback 端口/连接上限)", label, res.Stats.NetErr)
	}
	if os.Getenv("M6_DEBUG") == "1" {
		b.Logf("%s: lat n=%d mean=%.3fms min=%.3fms p50=%.3fms p95=%.3fms p99=%.3fms max=%.3fms",
			label, latN, m6Ms(mean), m6Ms(lo), m6Ms(p50), m6Ms(p95), m6Ms(p99), m6Ms(hi))
	}
	if res.Stats.Short > 0 {
		b.Logf("%s: short bodies=%d (200 但响应体长度不符，疑似流截断)", label, res.Stats.Short)
	}
	if st.Streams.Active != 0 {
		b.Logf("%s: streams.active=%d after drain (在途流未归零)", label, st.Streams.Active)
	}
	if !st.Up {
		b.Logf("%s: node up=false at snapshot", label)
	}
}

// --- 环境变量驱动的矩阵参数 ------------------------------------------------

func m6EnvInts(name string, def []int) []int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	var out []int
	for _, f := range strings.Split(raw, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		v, err := strconv.Atoi(f)
		if err == nil && v > 0 {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

func m6EnvDuration(name string, def time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

// m6Matrix 返回容量矩阵参数；-short 时收缩为冒烟矩阵（可用环境变量覆盖）。
func m6Matrix() (channels, streams, concs []int, window, warm time.Duration) {
	channels = m6EnvInts("M6_CHANNELS", []int{1, 2, 4})
	streams = m6EnvInts("M6_STREAMS", []int{32, 128, 256})
	concs = m6EnvInts("M6_CONC", []int{10, 50, 100, 200})
	window = m6EnvDuration("M6_WINDOW", 3*time.Second)
	warm = m6EnvDuration("M6_WARMUP", 300*time.Millisecond)
	if testing.Short() {
		if os.Getenv("M6_CHANNELS") == "" {
			channels = []int{1, 2}
		}
		if os.Getenv("M6_STREAMS") == "" {
			streams = []int{32, 256}
		}
		if os.Getenv("M6_CONC") == "" {
			concs = []int{10, 100}
		}
		if os.Getenv("M6_WINDOW") == "" {
			window = time.Second
		}
		if os.Getenv("M6_WARMUP") == "" {
			warm = 200 * time.Millisecond
		}
	}
	return channels, streams, concs, window, warm
}

func m6WaitFor(tb testing.TB, d time.Duration, what string, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	tb.Fatalf("timeout after %v waiting for %s", d, what)
}

// --- Benchmark：容量配比矩阵 -------------------------------------------------

// BenchmarkTunnelCapacity 遍历 channels × max_streams_per_conn × 并发，负载为 512B 小响应。
// 每个场景独立起一套 Caddy（新建 Caddy + stubclient），因此 /status 的峰值统计
// （peak_demand、channels_needed_at_peak、saturated_total）是该场景独占的。
func BenchmarkTunnelCapacity(b *testing.B) {
	if b.N != 1 {
		b.Skip("M6 固定窗口基准：请用 -benchtime=1x 运行（矩阵内部自行控制窗口时长）")
	}
	channels, streams, concs, window, warm := m6Matrix()
	m6EmitEnv(window, warm)

	echoAddr := m6StartEcho(b)
	sizes := map[string]int{
		m6PathSmall:   m6SmallBytes,
		m6PathBig:     m6BigBytes,
		m6PathTrickle: m6BigBytes,
	}

	for _, ch := range channels {
		for _, ms := range streams {
			for _, conc := range concs {
				label := fmt.Sprintf("ch%d_ms%d_c%d", ch, ms, conc)
				b.Run(label, func(b *testing.B) {
					if b.N != 1 {
						b.Skip("M6 固定窗口基准：请用 -benchtime=1x 运行")
					}
					env := m6StartEnv(b, ch, ms, echoAddr)

					// 预热窗口：排除握手/首个请求/WARMUP 抖动的影响。
					m6RunLoad(b, env.load, env.proxyURL, m6LoadCfg{
						Workers: conc, Duration: warm, PathSizes: sizes,
					})
					res := m6RunLoad(b, env.load, env.proxyURL, m6LoadCfg{
						Workers: conc, Duration: window, PathSizes: sizes,
					})
					st, err := m6StatusAfterDrain(b, env.statusURL)
					if err != nil {
						b.Fatalf("%s: fetch /status: %v", label, err)
					}
					m6EmitResult(b, "capacity", label, ch, ms, conc, "small512", window, res, st)
				})
			}
		}
	}

	// 若后面还有 HOL / 基线 benchmark 要跑，收尾等待交给它们统一做。
}

// --- Benchmark：队头阻塞（HOL）观察 ------------------------------------------

type m6HOLCase struct {
	name       string
	channels   int
	maxStreams int
	workers    int
	bigEvery   int // 每 N 个请求掺一个 64KB 大响应
	slow       int // 末尾 N 个 worker 打慢速 64KB（trickle）
	mix        string
}

// BenchmarkTunnelHOL 对比"单通道 vs 多通道"在混合大响应下的短请求 p99：
//   - floor：1 并发，单请求延迟下限（loopback 基线）
//   - baseline：纯 512B 短响应，作为同并发下的 p99 基线
//   - big10 / big50：按比例掺 64KB 大响应，观察短请求 p99 是否被同通道大流拖慢
//   - trickle：2 条慢速 64KB 长流 + 短请求，观察长寿命流的影响
func BenchmarkTunnelHOL(b *testing.B) {
	if b.N != 1 {
		b.Skip("M6 固定窗口基准：请用 -benchtime=1x 运行（矩阵内部自行控制窗口时长）")
	}
	_, _, _, window, warm := m6Matrix()
	m6EmitEnv(window, warm)

	echoAddr := m6StartEcho(b)
	sizes := map[string]int{
		m6PathSmall:   m6SmallBytes,
		m6PathBig:     m6BigBytes,
		m6PathTrickle: m6BigBytes,
	}

	cases := []m6HOLCase{
		{name: "floor_c1_ch1", channels: 1, maxStreams: 256, workers: 1, mix: "small512_only_1w"},
		{name: "baseline_c50_ch1", channels: 1, maxStreams: 256, workers: 50, mix: "small512_only_50w"},
		{name: "baseline_c50_ch4", channels: 4, maxStreams: 256, workers: 50, mix: "small512_only_50w"},
		{name: "big10_ch1", channels: 1, maxStreams: 256, workers: 50, bigEvery: 10, mix: "small512+big64k_10pct_50w"},
		{name: "big10_ch2", channels: 2, maxStreams: 256, workers: 50, bigEvery: 10, mix: "small512+big64k_10pct_50w"},
		{name: "big10_ch4", channels: 4, maxStreams: 256, workers: 50, bigEvery: 10, mix: "small512+big64k_10pct_50w"},
		{name: "big50_ch1", channels: 1, maxStreams: 256, workers: 100, bigEvery: 2, mix: "small512+big64k_50pct_100w"},
		{name: "big50_ch2", channels: 2, maxStreams: 256, workers: 100, bigEvery: 2, mix: "small512+big64k_50pct_100w"},
		{name: "big50_ch4", channels: 4, maxStreams: 256, workers: 100, bigEvery: 2, mix: "small512+big64k_50pct_100w"},
		{name: "trickle_ch1", channels: 1, maxStreams: 256, workers: 48, slow: 2, mix: "small512+trickle64k_x2_48w"},
		{name: "trickle_ch4", channels: 4, maxStreams: 256, workers: 48, slow: 2, mix: "small512+trickle64k_x2_48w"},
	}
	if testing.Short() {
		var filtered []m6HOLCase
		for _, c := range cases {
			if strings.Contains(c.name, "big10") || strings.HasPrefix(c.name, "floor") {
				filtered = append(filtered, c)
			}
		}
		cases = filtered
	}

	for _, c := range cases {
		c := c
		b.Run(c.name, func(b *testing.B) {
			if b.N != 1 {
				b.Skip("M6 固定窗口基准：请用 -benchtime=1x 运行")
			}
			env := m6StartEnv(b, c.channels, c.maxStreams, echoAddr)
			cfg := m6LoadCfg{
				Workers: c.workers, Duration: warm,
				BigEvery: c.bigEvery, SlowWorkers: c.slow, PathSizes: sizes,
			}
			m6RunLoad(b, env.load, env.proxyURL, cfg)
			cfg.Duration = window
			res := m6RunLoad(b, env.load, env.proxyURL, cfg)
			st, err := m6StatusAfterDrain(b, env.statusURL)
			if err != nil {
				b.Fatalf("%s: fetch /status: %v", c.name, err)
			}
			m6EmitResult(b, "hol", c.name, c.channels, c.maxStreams, c.workers, c.mix, window, res, st)
		})
	}
}

// --- Benchmark：压测台对照组（直连 echo，不经隧道） ---------------------------

// BenchmarkEchoDirectBaseline 是"测试台天花板"对照：同样的固定并发 worker + 固定窗口，
// 但目标直接是本地 echo（不经 Caddy/WS/smux），用于判断隧道容量是否已被压测台本身
// （同一进程内的负载发生器 + echo server + Windows loopback）限制。
//
// 这里用连接复用（keep-alive）而不是每请求新建连接：实测 Windows loopback 上
// "每请求新建连接"的直连对照会撞 ephemeral port 耗尽
// （connectex: Only one usage of each socket address ...），数据不可复现；因此该对照
// 只作为"无隧道上限"，与隧道路径（stub 侧每请求 dial echo）不是严格同构，报告里注明。
func BenchmarkEchoDirectBaseline(b *testing.B) {
	if b.N != 1 {
		b.Skip("M6 固定窗口基准：请用 -benchtime=1x 运行")
	}
	_, _, _, window, warm := m6Matrix()
	m6EmitEnv(window, warm)

	echoAddr := m6StartEcho(b)
	sizes := map[string]int{m6PathSmall: m6SmallBytes}
	baseURL := "http://" + echoAddr

	for _, conc := range []int{50, 200} {
		conc := conc
		b.Run(fmt.Sprintf("c%d", conc), func(b *testing.B) {
			if b.N != 1 {
				b.Skip("M6 固定窗口基准：请用 -benchtime=1x 运行")
			}
			tr := &http.Transport{
				DialContext:         (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
				MaxIdleConns:        1024,
				MaxIdleConnsPerHost: 1024,
				IdleConnTimeout:     30 * time.Second,
				DisableCompression:  true,
				ForceAttemptHTTP2:   false,
			}
			cl := &http.Client{Transport: tr, Timeout: 15 * time.Second}
			defer tr.CloseIdleConnections()

			m6RunLoad(b, cl, baseURL, m6LoadCfg{Workers: conc, Duration: warm, PathSizes: sizes})
			res := m6RunLoad(b, cl, baseURL, m6LoadCfg{Workers: conc, Duration: window, PathSizes: sizes})

			p50, p95, p99 := m6Quantiles(res.Stats.LatSmall)
			latN, mean, lo, hi := m6LatStats(res.Stats.LatSmall)
			m6EmitRow(m6Row{
				Bench: "baseline", Label: fmt.Sprintf("echo_direct_c%d", conc),
				Channels: 0, MaxStreams: 0, MaxConc: 0, Concurrency: conc,
				Mix: "small512_no_tunnel", Window: window,
				Elapsed: res.Elapsed, Stats: res.Stats,
				P50: p50, P95: p95, P99: p99,
				LatN: latN, LatMean: mean, LatMin: lo, LatMax: hi,
			})

			done := res.Stats.OK + res.Stats.Short + res.Stats.HTTP502 + res.Stats.HTTP503 + res.Stats.HTTPElse
			rps := 0.0
			if res.Elapsed > 0 {
				rps = float64(done) / res.Elapsed.Seconds()
			}
			b.ReportMetric(rps, "req/s")
			b.ReportMetric(m6Ms(p50), "p50ms")
			b.ReportMetric(m6Ms(p95), "p95ms")
			b.ReportMetric(m6Ms(p99), "p99ms")
		})
	}
}
