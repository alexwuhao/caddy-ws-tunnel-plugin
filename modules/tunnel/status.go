package tunnel

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"
)

func init() {
	caddy.RegisterModule(TunnelStatus{})
}

// TunnelStatus serves GET /status[?node=] — a human-readable JSON snapshot
// matching design §12 exactly.
type TunnelStatus struct {
	AuthToken string `json:"auth_token,omitempty"`

	app *TunnelApp
	log *zap.Logger
}

// CaddyModule returns the Caddy module information.
func (TunnelStatus) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.tunnel_status",
		New: func() caddy.Module { return new(TunnelStatus) },
	}
}

func (s *TunnelStatus) Provision(ctx caddy.Context) error {
	s.log = ctx.Logger(s)
	appIface, err := ctx.App("tunnel")
	if err != nil {
		return fmt.Errorf("loading tunnel app: %w", err)
	}
	s.app = appIface.(*TunnelApp)
	return nil
}

func (s *TunnelStatus) ServeHTTP(w http.ResponseWriter, r *http.Request, _ caddyhttp.Handler) error {
	if s.AuthToken != "" && r.Header.Get("Authorization") != "Bearer "+s.AuthToken {
		w.WriteHeader(http.StatusUnauthorized)
		return nil
	}

	filterNode := r.URL.Query().Get("node")
	nodes := s.app.registry.all()

	type controlStatus struct {
		Connected bool   `json:"connected"`
		LastSeen  string `json:"last_seen"`
		RttMs     int64  `json:"rtt_ms"`
	}
	type channelStatus struct {
		Configured int `json:"configured"`
		Online     int `json:"online"`
	}
	type streamStatus struct {
		Active      int64 `json:"active"`
		OpenedTotal int64 `json:"opened_total"`
		QueueDepth  int64 `json:"queue_depth"`
	}
	type capacityStatus struct {
		MaxStreamsPerConn     int     `json:"max_streams_per_conn"`
		MaxConcurrent        int     `json:"max_concurrent"`
		PeakDemand           int64   `json:"peak_demand"`
		ChannelsNeededAtPeak int     `json:"channels_needed_at_peak"`
		PeakUtilization      float64 `json:"peak_utilization"`
		SaturatedTotal       int64   `json:"saturated_total"`
	}
	type trafficStatus struct {
		BytesIn   int64 `json:"bytes_in"`
		BytesOut  int64 `json:"bytes_out"`
		RateInBps int64 `json:"rate_in_bps"`
		RateOutBps int64 `json:"rate_out_bps"`
	}
	type nodeSnapshot struct {
		Node      string         `json:"node"`
		Up        bool           `json:"up"`
		Control   controlStatus  `json:"control"`
		Channels  channelStatus  `json:"channels"`
		Streams   streamStatus   `json:"streams"`
		Capacity  capacityStatus `json:"capacity"`
		Traffic   trafficStatus  `json:"traffic"`
		Ports     []string       `json:"ports"`
		LastError *string        `json:"last_error"`
	}

	var snapshots []nodeSnapshot
	for _, n := range nodes {
		if filterNode != "" && n.Name != filterNode {
			continue
		}
		n.mu.RLock()
		up := n.control != nil && n.controlDown.IsZero()
		ctrl := controlStatus{Connected: up}
		if n.control != nil {
			ctrl.LastSeen = n.control.lastSeenTime().UTC().Format("2006-01-02T15:04:05Z")
			ctrl.RttMs = n.control.rtt()
		}
		onlineCnt := 0
		var active int64
		var totalIn, totalOut int64
		for _, ch := range n.channels {
			if ch.online() {
				onlineCnt++
			}
			active += atomic.LoadInt64(&ch.active)
			in, out := ch.trafficBytes()
			totalIn += in
			totalOut += out
		}
		ports := make([]string, 0, len(n.cfg.Ports))
		for p := range n.cfg.Ports {
			ports = append(ports, p)
		}
		maxConc := n.cfg.Channels * n.cfg.MaxStreamsPerConn
		var peakUtil float64
		if maxConc > 0 {
			peakUtil = float64(n.peakDemand) / float64(maxConc)
		}
		var lastErr *string
		if n.lastError != "" {
			s := n.lastError
			lastErr = &s
		}
		snap := nodeSnapshot{
			Node:     n.Name,
			Up:       up,
			Control:  ctrl,
			Channels: channelStatus{Configured: n.cfg.Channels, Online: onlineCnt},
			Streams:  streamStatus{Active: active, OpenedTotal: n.openedTotal, QueueDepth: n.queued},
			Capacity: capacityStatus{
				MaxStreamsPerConn:     n.cfg.MaxStreamsPerConn,
				MaxConcurrent:        maxConc,
				PeakDemand:           n.peakDemand,
				ChannelsNeededAtPeak: n.channelsNeededAtPeak,
				PeakUtilization:      peakUtil,
				SaturatedTotal:       n.saturatedTotal,
			},
			Traffic:   trafficStatus{BytesIn: totalIn, BytesOut: totalOut},
			Ports:     ports,
			LastError: lastErr,
		}
		n.mu.RUnlock()
		snapshots = append(snapshots, snap)
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(map[string]any{"nodes": snapshots})
}

// Interface guards.
var (
	_ caddy.Provisioner           = (*TunnelStatus)(nil)
	_ caddyhttp.MiddlewareHandler = (*TunnelStatus)(nil)
)
