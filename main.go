package main

import (
	"context"
	"crypto/tls"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed static/*
var staticFiles embed.FS

type Config struct {
	Host          string `json:"host"`
	VDOM          string `json:"vdom"`
	APIToken      string `json:"apiToken"`
	Username      string `json:"username"`
	Password      string `json:"password"`
	InsecureTLS   bool   `json:"insecureTLS"`
	PollSeconds   int    `json:"pollSeconds"`
	WindowMinutes int    `json:"windowMinutes"`
	GroupBy       string `json:"groupBy"`
	TopN          int    `json:"topN"`
	DemoMode      bool   `json:"demoMode"`
	Endpoint      string `json:"endpoint"`

	ResolveDevices       bool   `json:"resolveDevices"`
	DeviceResolveSeconds int    `json:"deviceResolveSeconds"`
	DeviceEndpoint       string `json:"deviceEndpoint"`
	DeviceCommand        string `json:"deviceCommand"`
	ResolveExternalDNS   bool   `json:"resolveExternalDns"`
	DNSServer            string `json:"dnsServer"`
	DNSCacheMinutes      int    `json:"dnsCacheMinutes"`
	SaveSettings         bool   `json:"saveSettings"`
	SaveSecrets          bool   `json:"saveSecrets"`
}
type Bucket struct {
	T          int64              `json:"t"`
	Series     map[string]float64 `json:"series"`
	Interfaces map[string]string  `json:"interfaces,omitempty"`
	Total      float64            `json:"total"`
}

type Status struct {
	Running      bool    `json:"running"`
	LastPoll     string  `json:"lastPoll"`
	LastError    string  `json:"lastError"`
	SessionCount int     `json:"sessionCount"`
	TotalMbps    float64 `json:"totalMbps"`
}

type Conversation struct {
	Label           string  `json:"label"`
	SourceIP        string  `json:"sourceIp"`
	SourceName      string  `json:"sourceName"`
	DestinationIP   string  `json:"destinationIp"`
	DestinationName string  `json:"destinationName"`
	DestinationPort string  `json:"destinationPort"`
	EgressInterface string  `json:"egressInterface"`
	Bps             float64 `json:"bps"`
}

type SessionSample struct {
	Key          string
	Bytes        uint64
	Group        string
	Conversation Conversation
}

type nameCacheEntry struct {
	Name string
	T    time.Time
}

type App struct {
	mu                   sync.Mutex
	cfg                  Config
	running              bool
	cancel               context.CancelFunc
	buckets              []Bucket
	prev                 map[string]SessionSample
	status               Status
	currentConversations []Conversation
	clients              map[chan []byte]bool
	deviceNames          map[string]string
	dnsCache             map[string]nameCacheEntry
	lastDeviceResolve    time.Time
}

func main() {
	listen := flag.String("listen", ":8080", "listen address")
	flag.Parse()

	app := &App{
		cfg:         defaultConfig(),
		prev:        map[string]SessionSample{},
		clients:     map[chan []byte]bool{},
		deviceNames: map[string]string{},
		dnsCache:    map[string]nameCacheEntry{},
	}
	if saved, err := loadSavedConfig(); err == nil {
		normalizeConfig(&saved)
		app.cfg = saved
	}

	mux := http.NewServeMux()
	mux.Handle("/static/", http.FileServer(http.FS(staticFiles)))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		b, err := staticFiles.ReadFile("static/index.html")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(b)
	})
	mux.HandleFunc("/api/config", app.handleConfig)
	mux.HandleFunc("/api/start", app.handleStart)
	mux.HandleFunc("/api/stop", app.handleStop)
	mux.HandleFunc("/api/snapshot", app.handleSnapshot)
	mux.HandleFunc("/api/save-settings", app.handleSaveSettings)
	mux.HandleFunc("/events", app.handleEvents)

	log.Printf("FortiGate LiveFlow listening on http://localhost%s", *listen)
	log.Fatal(http.ListenAndServe(*listen, mux))
}

func (a *App) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		a.mu.Lock()
		cfg := a.cfg
		cfg.APIToken = ""
		cfg.Password = ""
		a.mu.Unlock()
		writeJSON(w, cfg)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var cfg Config
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	normalizeConfig(&cfg)
	a.mu.Lock()
	running := a.running
	old := a.cfg
	if cfg.APIToken == "" {
		cfg.APIToken = old.APIToken
	}
	if cfg.Password == "" {
		cfg.Password = old.Password
	}
	a.cfg = cfg
	a.mu.Unlock()
	if cfg.SaveSettings {
		_ = saveConfig(cfg)
	}
	if running {
		a.stop()
		_ = a.start()
	}
	writeJSON(w, map[string]any{"ok": true})
}

func (a *App) handleSaveSettings(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	a.mu.Lock()
	cfg := a.cfg
	a.mu.Unlock()
	if err := saveConfig(cfg); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, map[string]any{"ok": true, "path": configPath()})
}

func (a *App) handleStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	if err := a.start(); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func (a *App) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	a.stop()
	writeJSON(w, map[string]any{"ok": true})
}

func (a *App) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	snap := map[string]any{"config": sanitized(a.cfg), "status": a.status, "buckets": append([]Bucket(nil), a.buckets...), "conversations": append([]Conversation(nil), a.currentConversations...)}
	a.mu.Unlock()
	writeJSON(w, snap)
}

func (a *App) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}
	ch := make(chan []byte, 16)
	a.mu.Lock()
	a.clients[ch] = true
	a.mu.Unlock()
	defer func() { a.mu.Lock(); delete(a.clients, ch); a.mu.Unlock(); close(ch) }()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	fmt.Fprintf(w, ": connected\n\n")
	flusher.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		case <-time.After(25 * time.Second):
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

func (a *App) start() error {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return nil
	}
	cfg := a.cfg
	if !cfg.DemoMode && strings.TrimSpace(cfg.Host) == "" {
		a.mu.Unlock()
		return errors.New("host is required unless demo mode is enabled")
	}
	ctx, cancel := context.WithCancel(context.Background())
	a.cancel = cancel
	a.running = true
	a.prev = map[string]SessionSample{}
	a.buckets = nil
	a.status = Status{Running: true}
	a.currentConversations = nil
	a.mu.Unlock()
	go a.pollLoop(ctx, cfg)
	return nil
}

func (a *App) stop() {
	a.mu.Lock()
	cancel := a.cancel
	a.running = false
	a.status.Running = false
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	a.broadcastSnapshot()
}

func (a *App) pollLoop(ctx context.Context, cfg Config) {
	ticker := time.NewTicker(time.Duration(cfg.PollSeconds) * time.Second)
	defer ticker.Stop()
	a.pollOnce(ctx, cfg)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.pollOnce(ctx, cfg)
		}
	}
}

func (a *App) pollOnce(ctx context.Context, cfg Config) {
	samples, err := collectSessions(ctx, cfg)
	now := time.Now().Unix()
	series := map[string]float64{}
	seriesInterfaces := map[string]string{}
	convoBps := map[string]float64{}
	convoMeta := map[string]Conversation{}
	total := 0.0
	a.mu.Lock()
	prev := a.prev
	nextPrev := make(map[string]SessionSample, len(samples))
	for _, s := range samples {
		nextPrev[s.Key] = s
		if p, ok := prev[s.Key]; ok && s.Bytes >= p.Bytes {
			bps := float64(s.Bytes-p.Bytes) * 8 / float64(cfg.PollSeconds)
			if bps > 0 {
				g := s.Group
				if g == "" {
					g = "unknown"
				}
				series[g] += bps
				if s.Conversation.EgressInterface != "" {
					seriesInterfaces[g] = s.Conversation.EgressInterface
				}
				if s.Conversation.Label != "" {
					convoBps[s.Conversation.Label] += bps
					convoMeta[s.Conversation.Label] = s.Conversation
				}
				total += bps
			}
		}
	}
	a.prev = nextPrev
	currentConversations := a.decorateConversations(ctx, cfg, conversationsFromMaps(convoBps, convoMeta))
	bucket := Bucket{T: now, Series: series, Interfaces: seriesInterfaces, Total: total}
	if len(prev) > 0 {
		a.buckets = append(a.buckets, bucket)
	}
	cutoff := now - int64(cfg.WindowMinutes*60)
	keep := a.buckets[:0]
	for _, b := range a.buckets {
		if b.T >= cutoff {
			keep = append(keep, b)
		}
	}
	a.buckets = keep
	lastErr := ""
	if err != nil {
		lastErr = err.Error()
	}
	a.currentConversations = currentConversations
	a.status = Status{Running: a.running, LastPoll: time.Now().Format(time.RFC3339), LastError: lastErr, SessionCount: len(samples), TotalMbps: total / 1000000}
	a.mu.Unlock()
	a.broadcastSnapshot()
}

func (a *App) decorateConversations(ctx context.Context, cfg Config, convs []Conversation) []Conversation {
	if len(convs) == 0 {
		return convs
	}
	now := time.Now()
	if cfg.ResolveDevices && (a.lastDeviceResolve.IsZero() || now.Sub(a.lastDeviceResolve) >= time.Duration(cfg.DeviceResolveSeconds)*time.Second) {
		if names, err := collectDeviceNames(ctx, cfg); err == nil {
			a.deviceNames = names
			a.lastDeviceResolve = now
		} else {
			a.status.LastError = "device resolve: " + err.Error()
		}
	}
	for i := range convs {
		if cfg.ResolveDevices {
			if n := a.deviceNames[convs[i].SourceIP]; n != "" {
				convs[i].SourceName = n
			}
		}
		if cfg.ResolveExternalDNS && cfg.DNSServer != "" && isExternalIP(convs[i].DestinationIP) {
			if n := a.resolvePTR(ctx, convs[i].DestinationIP, cfg); n != "" {
				convs[i].DestinationName = n
			}
		}
	}
	return convs
}

func collectSessions(ctx context.Context, cfg Config) ([]SessionSample, error) {
	if cfg.DemoMode {
		return demoSessions(cfg), nil
	}
	client, err := makeHTTPClient(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.APIToken == "" && cfg.Username != "" {
		if err := loginFortiGate(ctx, client, cfg); err != nil {
			return nil, err
		}
	}
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = "/api/v2/monitor/firewall/session?count=1000"
	}
	u := strings.TrimRight(cfg.Host, "/") + endpoint
	q := url.Values{}
	if cfg.VDOM != "" {
		q.Set("vdom", cfg.VDOM)
	}
	if strings.Contains(u, "?") {
		u += "&" + q.Encode()
	} else {
		u += "?" + q.Encode()
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if cfg.APIToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.APIToken)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("FortiGate API returned %s: %s", resp.Status, truncate(string(body), 240))
	}
	var raw any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	return extractSamples(raw, cfg.GroupBy), nil
}

func makeHTTPClient(cfg Config) (*http.Client, error) {
	jar, _ := cookiejar.New(nil)
	return &http.Client{Timeout: 20 * time.Second, Jar: jar, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: cfg.InsecureTLS}}}, nil
}

func loginFortiGate(ctx context.Context, client *http.Client, cfg Config) error {
	form := url.Values{"username": {cfg.Username}, "secretkey": {cfg.Password}}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(cfg.Host, "/")+"/logincheck", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if resp.StatusCode >= 300 || strings.Contains(strings.ToLower(string(body)), "failed") {
		return fmt.Errorf("login failed: HTTP %s", resp.Status)
	}
	return nil
}

func extractSamples(raw any, groupBy string) []SessionSample {
	rows := findLikelyRows(raw)
	samples := []SessionSample{}
	for i, row := range rows {
		m, ok := row.(map[string]any)
		if !ok {
			continue
		}
		bytes := firstUint(m, []string{"bytes", "total_bytes", "byte", "sentbyte", "rcvdbyte", "sent_bytes", "rcvd_bytes", "tx_bytes", "rx_bytes", "orgsentbyte", "revsentbyte"})
		if bytes == 0 {
			bytes = firstUint(m, []string{"sentbyte", "sent_bytes", "tx_bytes", "orgsentbyte"}) + firstUint(m, []string{"rcvdbyte", "rcvd_bytes", "rx_bytes", "revsentbyte"})
		}
		key := firstString(m, []string{"id", "session_id", "serial", "sesid", "uuid"})
		if key == "" {
			key = strings.Join([]string{firstString(m, []string{"srcip", "src", "src_ip"}), firstString(m, []string{"dstip", "dst", "dst_ip"}), firstString(m, []string{"sport", "src_port"}), firstString(m, []string{"dport", "dst_port"}), firstString(m, []string{"proto", "protocol"}), firstString(m, []string{"policyid", "policy_id"})}, ":")
		}
		if key == ":::::" {
			key = fmt.Sprintf("row-%d", i)
		}
		conv := conversationFromRow(m)
		samples = append(samples, SessionSample{Key: key, Bytes: bytes, Group: groupName(m, groupBy), Conversation: conv})
	}
	return samples
}

func findLikelyRows(v any) []any {
	best := []any{}
	var walk func(any)
	walk = func(x any) {
		switch t := x.(type) {
		case []any:
			score := 0
			for _, item := range t {
				if m, ok := item.(map[string]any); ok && (hasAny(m, "saddr", "daddr", "srcip", "src", "dstip", "bytes", "sentbyte", "rcvdbyte", "policyid")) {
					score++
				}
			}
			if score > len(best) {
				best = t
			}
			for _, item := range t {
				walk(item)
			}
		case map[string]any:
			for _, item := range t {
				walk(item)
			}
		}
	}
	walk(v)
	return best
}

func groupName(m map[string]any, by string) string {
	src := firstString(m, []string{"saddr", "srcaddr", "srcip", "src", "src_ip", "src-addr", "source", "sourceip", "source_ip", "sourceaddress"})
	dst := firstString(m, []string{"daddr", "dstaddr", "dstip", "dst", "dst_ip", "dst-addr", "destination", "destinationip", "destination_ip", "destinationaddress"})
	dport := firstString(m, []string{"dport", "dstport", "dst_port", "destinationport", "destination_port", "serviceport", "service_port"})
	sport := firstString(m, []string{"sport", "srcport", "src_port", "sourceport", "source_port"})
	proto := firstString(m, []string{"proto", "protocol"})
	switch by {
	case "conversation":
		name := strings.TrimSpace(src + " → " + dst)
		if dport != "" {
			name += ":" + dport
		}
		if proto != "" {
			name += " " + proto
		}
		return strings.TrimSpace(name)
	case "flow":
		name := strings.TrimSpace(src + " → " + dst)
		if sport != "" && dport != "" {
			name += " " + sport + "→" + dport
		}
		return strings.TrimSpace(name)
	case "destination":
		return dst
	case "policy":
		p := firstString(m, []string{"policyid", "policy_id", "policy", "policyid"})
		if p == "" {
			return ""
		}
		return "Policy " + p
	case "application":
		return firstString(m, []string{"app", "application", "app_name", "appid", "appcat", "service"})
	case "user":
		return firstString(m, []string{"user", "username", "authuser", "unauthuser", "srcuser"})
	case "interface":
		return firstString(m, []string{"srcintf", "srcintfrole", "dstintf", "interface", "srcintfname", "dstintfname"})
	default:
		return src
	}
}

func conversationFromRow(m map[string]any) Conversation {
	src := firstString(m, []string{"saddr", "srcaddr", "srcip", "src", "src_ip", "src-addr", "source", "sourceip", "source_ip", "sourceaddress", "source-address", "clientip", "client_ip"})
	dst := firstString(m, []string{"daddr", "dstaddr", "dstip", "dst", "dst_ip", "dst-addr", "destination", "destinationip", "destination_ip", "destinationaddress", "destination-address", "serverip", "server_ip", "remip", "remoteip", "remote_ip"})
	dport := firstString(m, []string{"dport", "dstport", "dst_port", "destinationport", "destination_port", "serviceport", "service_port", "serverport", "server_port", "remport", "remoteport", "remote_port"})
	egress := firstString(m, []string{"dstintf", "dstintfname", "dst_intf", "dstinterface", "destinationinterface", "destination_interface", "outintf", "out_intf", "outgoinginterface", "egressinterface", "egress_interface", "egress", "wan", "interface"})
	label := strings.TrimSpace(src + " → " + dst)
	if dport != "" {
		label += ":" + dport
	}
	if label == "→" || label == "→:" || strings.TrimSpace(label) == "" {
		label = firstString(m, []string{"id", "session_id", "sesid", "uuid"})
	}
	return Conversation{Label: strings.TrimSpace(label), SourceIP: src, DestinationIP: dst, DestinationPort: dport, EgressInterface: egress}
}

func conversationsFromMaps(bps map[string]float64, meta map[string]Conversation) []Conversation {
	out := make([]Conversation, 0, len(bps))
	for label, v := range bps {
		c := meta[label]
		c.Label = label
		c.Bps = v
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Bps > out[j].Bps })
	if len(out) > 200 {
		out = out[:200]
	}
	return out
}

func normKey(s string) string {
	s = strings.ToLower(s)
	repl := strings.NewReplacer("_", "", "-", "", ".", "", " ", "")
	return repl.Replace(s)
}

func findValue(m map[string]any, keys []string) (any, bool) {
	want := map[string]bool{}
	for _, k := range keys {
		want[normKey(k)] = true
	}
	var walk func(any) (any, bool)
	walk = func(x any) (any, bool) {
		mm, ok := x.(map[string]any)
		if !ok {
			return nil, false
		}
		for k, v := range mm {
			if want[normKey(k)] {
				return v, true
			}
		}
		for _, v := range mm {
			if child, ok := v.(map[string]any); ok {
				if found, yes := walk(child); yes {
					return found, true
				}
			}
		}
		return nil, false
	}
	return walk(m)
}

func firstUint(m map[string]any, keys []string) uint64 {
	var sum uint64
	for _, k := range keys {
		if v, ok := findValue(m, []string{k}); ok {
			sum += toUint(v)
		}
	}
	return sum
}
func firstString(m map[string]any, keys []string) string {
	for _, k := range keys {
		if v, ok := findValue(m, []string{k}); ok {
			s := strings.TrimSpace(fmt.Sprint(v))
			if s != "" && s != "<nil>" {
				return s
			}
		}
	}
	return ""
}
func hasAny(m map[string]any, keys ...string) bool {
	_, ok := findValue(m, keys)
	return ok
}

func toUint(v any) uint64 {
	switch t := v.(type) {
	case float64:
		return uint64(t)
	case int:
		return uint64(t)
	case uint64:
		return t
	case json.Number:
		n, _ := strconv.ParseUint(t.String(), 10, 64)
		return n
	case string:
		n, _ := strconv.ParseUint(strings.TrimSpace(t), 10, 64)
		return n
	}
	return 0
}

func compressSeries(in map[string]float64, topN int) map[string]float64 {
	type kv struct {
		K string
		V float64
	}
	arr := []kv{}
	for k, v := range in {
		arr = append(arr, kv{k, v})
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i].V > arr[j].V })
	out := map[string]float64{}
	other := 0.0
	if topN < 1 {
		topN = 10
	}
	for i, x := range arr {
		if i < topN {
			out[x.K] = x.V
		} else {
			other += x.V
		}
	}
	if other > 0 {
		out["Other"] = other
	}
	return out
}

var demoState = map[string]uint64{}

func demoSessions(cfg Config) []SessionSample {
	groups := []string{"10.20.10.42", "10.20.10.51", "10.20.20.12", "Guest WiFi", "POS VLAN", "Cameras", "Digital Signage", "Back Office"}
	out := []SessionSample{}
	dests := []string{"8.8.8.8", "151.101.1.69", "17.253.144.10", "52.96.40.34", "10.10.10.5"}
	ports := []string{"53", "443", "443", "443", "3389"}
	intfs := []string{"wan1", "wan1", "wan2", "wan1", "internal"}
	for i := 0; i < 120; i++ {
		g := groups[i%len(groups)]
		dst := dests[i%len(dests)]
		port := ports[i%len(ports)]
		label := fmt.Sprintf("%s → %s:%s", g, dst, port)
		key := fmt.Sprintf("%s-%d", label, i)
		demoState[key] += uint64(8000 + rand.Intn(250000) + (i%7)*20000)
		out = append(out, SessionSample{Key: key, Group: label, Bytes: demoState[key], Conversation: Conversation{Label: label, SourceIP: g, DestinationIP: dst, DestinationPort: port, EgressInterface: intfs[i%len(intfs)]}})
	}
	return out
}

func defaultConfig() Config {
	return Config{VDOM: "root", PollSeconds: 5, WindowMinutes: 15, GroupBy: "conversation", TopN: 50, InsecureTLS: true, Endpoint: "/api/v2/monitor/firewall/session?count=1000", DeviceResolveSeconds: 300, DeviceCommand: "diagnose user device list", DeviceEndpoint: "/api/v2/monitor/user/device/query", DNSCacheMinutes: 60}
}

func normalizeConfig(c *Config) {
	if c.PollSeconds < 5 {
		c.PollSeconds = 5
	}
	if c.PollSeconds > 300 {
		c.PollSeconds = 300
	}
	if c.WindowMinutes < 1 {
		c.WindowMinutes = 15
	}
	if c.WindowMinutes > 240 {
		c.WindowMinutes = 240
	}
	if c.TopN < 3 {
		c.TopN = 10
	}
	if c.TopN > 25 {
		c.TopN = 25
	}
	if c.VDOM == "" {
		c.VDOM = "root"
	}
	if c.Endpoint == "" {
		c.Endpoint = "/api/v2/monitor/firewall/session?count=1000"
	}
	if c.DeviceResolveSeconds < 30 {
		c.DeviceResolveSeconds = 300
	}
	if c.DeviceResolveSeconds > 3600 {
		c.DeviceResolveSeconds = 3600
	}
	if c.DeviceCommand == "" {
		c.DeviceCommand = "diagnose user device list"
	}
	if c.DeviceEndpoint == "" {
		c.DeviceEndpoint = "/api/v2/monitor/user/device/query"
	}
	if c.DNSCacheMinutes < 1 {
		c.DNSCacheMinutes = 60
	}
	if c.DNSCacheMinutes > 1440 {
		c.DNSCacheMinutes = 1440
	}
	if c.Host != "" && !strings.HasPrefix(c.Host, "http") {
		c.Host = "https://" + c.Host
	}
}
func sanitized(c Config) Config { c.APIToken = ""; c.Password = ""; return c }

func configPath() string {
	if dir, err := os.UserConfigDir(); err == nil {
		return filepath.Join(dir, "fortigate-liveflow", "settings.json")
	}
	return "fortigate-liveflow-settings.json"
}
func saveConfig(c Config) error {
	path := configPath()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	toSave := c
	if !toSave.SaveSecrets {
		toSave.APIToken = ""
		toSave.Password = ""
	}
	b, _ := json.MarshalIndent(toSave, "", "  ")
	return os.WriteFile(path, b, 0600)
}
func loadSavedConfig() (Config, error) {
	b, err := os.ReadFile(configPath())
	if err != nil {
		return Config{}, err
	}
	var c Config
	err = json.Unmarshal(b, &c)
	return c, err
}

func (a *App) resolvePTR(ctx context.Context, ip string, cfg Config) string {
	if ent, ok := a.dnsCache[ip]; ok && time.Since(ent.T) < time.Duration(cfg.DNSCacheMinutes)*time.Minute {
		return ent.Name
	}
	server := cfg.DNSServer
	if !strings.Contains(server, ":") {
		server += ":53"
	}
	d := net.Dialer{Timeout: 2 * time.Second}
	r := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		return d.DialContext(ctx, network, server)
	}}
	names, err := r.LookupAddr(ctx, ip)
	name := ""
	if err == nil && len(names) > 0 {
		name = strings.TrimSuffix(names[0], ".")
	}
	a.dnsCache[ip] = nameCacheEntry{Name: name, T: time.Now()}
	return name
}

func collectDeviceNames(ctx context.Context, cfg Config) (map[string]string, error) {
	if cfg.DemoMode {
		return map[string]string{"192.168.128.42": "Demo-iPad", "192.168.128.73": "Work-MacBook", "192.168.128.23": "Kitchen-TV", "10.20.10.42": "demo-laptop", "10.20.10.51": "demo-ipad"}, nil
	}
	client, err := makeHTTPClient(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.APIToken == "" && cfg.Username != "" {
		if err := loginFortiGate(ctx, client, cfg); err != nil {
			return nil, err
		}
	}
	endpoint := cfg.DeviceEndpoint
	if endpoint == "" {
		endpoint = "/api/v2/monitor/user/device/query"
	}
	u := strings.TrimRight(cfg.Host, "/") + endpoint
	q := url.Values{}
	if cfg.VDOM != "" {
		q.Set("vdom", cfg.VDOM)
	}
	if strings.Contains(u, "?") {
		u += "&" + q.Encode()
	} else {
		u += "?" + q.Encode()
	}

	method := http.MethodGet
	var body io.Reader
	if !strings.Contains(strings.ToLower(endpoint), "/user/device/query") {
		method = http.MethodPost
		body = strings.NewReader(fmt.Sprintf(`{"cmd":%q,"command":%q}`, cfg.DeviceCommand, cfg.DeviceCommand))
	}
	req, _ := http.NewRequestWithContext(ctx, method, u, body)
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	if cfg.APIToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.APIToken)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("FortiGate device lookup returned %s: %s", resp.Status, truncate(string(b), 220))
	}
	if strings.Contains(strings.ToLower(endpoint), "/user/device/query") {
		if m := parseDeviceQueryJSON(b); len(m) > 0 {
			return m, nil
		}
	}
	return parseDeviceListOutput(string(b)), nil
}

func parseDeviceQueryJSON(b []byte) map[string]string {
	out := map[string]string{}
	var raw any
	if json.Unmarshal(b, &raw) != nil {
		return out
	}
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case []any:
			for _, item := range x {
				walk(item)
			}
		case map[string]any:
			if ip := bestIPFromMap(x); ip != "" {
				if name := bestDeviceNameFromMap(x); name != "" && name != ip {
					out[ip] = name
				}
			}
			for _, item := range x {
				walk(item)
			}
		}
	}
	walk(raw)
	return out
}

func bestIPFromMap(m map[string]any) string {
	keys := []string{"ip", "ipaddr", "ip_addr", "ip-address", "ip_address", "ipv4", "ipv4_addr", "ipv4-address", "address", "addr"}
	for _, k := range keys {
		if s := stringFieldCI(m, k); s != "" {
			if ip := firstIPv4(s); ip != "" {
				return ip
			}
		}
	}
	return ""
}

func bestDeviceNameFromMap(m map[string]any) string {
	keys := []string{"host", "hostname", "host_name", "host-name", "dhcp_host", "dhcp-host", "dhcp_hostname", "dhcp-hostname", "device_name", "device-name", "devname", "name", "alias"}
	for _, k := range keys {
		if s := strings.TrimSpace(stringFieldCI(m, k)); s != "" && net.ParseIP(s) == nil {
			return strings.Trim(s, "'\"")
		}
	}
	return ""
}

func normalizeFieldName(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "_", "")
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, " ", "")
	return s
}

func stringFieldCI(m map[string]any, key string) string {
	want := normalizeFieldName(key)
	for k, v := range m {
		if normalizeFieldName(k) != want {
			continue
		}
		switch t := v.(type) {
		case string:
			return t
		case float64:
			return strconv.FormatFloat(t, 'f', -1, 64)
		case bool:
			if t {
				return "true"
			}
			return "false"
		}
	}
	return ""
}

func firstIPv4(s string) string {
	re := regexp.MustCompile(`\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b`)
	for _, candidate := range re.FindAllString(s, -1) {
		if ip := net.ParseIP(candidate); ip != nil && strings.Contains(candidate, ".") {
			return candidate
		}
	}
	return ""
}

func parseDeviceListOutput(s string) map[string]string {
	out := map[string]string{}
	var raw any
	if json.Unmarshal([]byte(s), &raw) == nil {
		s = collectStrings(raw)
	}

	// FortiOS "diagnose user device list" is normally formatted as repeated
	// blocks beginning with a line like:
	//   vd root/0  a0:d2:b1:48:bc:1e  gen 79584  req 0
	// The friendliest label is usually:
	//   host 'AmazonPlug0TH1'  src dhcp
	// and the IP is usually:
	//   ip 192.168.128.28  src arp
	blocks := splitDeviceBlocks(s)
	ipRe := regexp.MustCompile(`(?im)^\s*ip\s+([0-9]{1,3}(?:\.[0-9]{1,3}){3})\b`)
	hostQuotedRe := regexp.MustCompile(`(?im)^\s*host\s+'([^']+)'`)
	hostBareRe := regexp.MustCompile(`(?im)^\s*host\s+([^\r\n]+?)\s+(?:src|id|weight)\b`)
	fallbackNameRe := regexp.MustCompile(`(?im)^\s*(?:hostname|host name|device name|devname|name)\s*[:=]\s*([^\r\n,]+)`)

	for _, b := range blocks {
		ip := firstMatch(ipRe, b)
		name := strings.TrimSpace(firstMatch(hostQuotedRe, b))
		if name == "" {
			name = strings.TrimSpace(firstMatch(hostBareRe, b))
		}
		if name == "" {
			name = strings.TrimSpace(firstMatch(fallbackNameRe, b))
		}
		name = strings.Trim(name, "'\"")
		if ip != "" && name != "" {
			out[ip] = name
		}
	}
	return out
}

func splitDeviceBlocks(s string) []string {
	startRe := regexp.MustCompile(`(?m)^\s*vd\s+[^\r\n]+`)
	locs := startRe.FindAllStringIndex(s, -1)
	if len(locs) == 0 {
		return regexp.MustCompile(`\n\s*\n`).Split(s, -1)
	}
	blocks := make([]string, 0, len(locs))
	for i, loc := range locs {
		start := loc[0]
		end := len(s)
		if i+1 < len(locs) {
			end = locs[i+1][0]
		}
		blocks = append(blocks, s[start:end])
	}
	return blocks
}
func collectStrings(v any) string {
	var parts []string
	var walk func(any)
	walk = func(x any) {
		switch t := x.(type) {
		case string:
			parts = append(parts, t)
		case []any:
			for _, v := range t {
				walk(v)
			}
		case map[string]any:
			for _, v := range t {
				walk(v)
			}
		}
	}
	walk(v)
	return strings.Join(parts, "\n")
}
func firstMatch(r *regexp.Regexp, s string) string {
	m := r.FindStringSubmatch(s)
	if len(m) > 1 {
		return m[1]
	}
	return ""
}
func isExternalIP(s string) bool {
	ip := net.ParseIP(s)
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsMulticast() {
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}
func (a *App) broadcastSnapshot() {
	a.mu.Lock()
	payload, _ := json.Marshal(map[string]any{"config": sanitized(a.cfg), "status": a.status, "buckets": a.buckets, "conversations": a.currentConversations})
	clients := make([]chan []byte, 0, len(a.clients))
	for c := range a.clients {
		clients = append(clients, c)
	}
	a.mu.Unlock()
	for _, c := range clients {
		select {
		case c <- payload:
		default:
		}
	}
}
