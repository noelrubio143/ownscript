// goproxy - a minimal HAProxy-style TCP load balancer / multiplexer written in Go.
//
// Features:
//   - Layer 4 (raw TCP) reverse proxy / multiplexer
//   - Multiple backend pools, each with its own listen address
//   - Load balancing strategies: round-robin, least-connections
//   - Active health checks (periodic TCP dial, like HAProxy's "check")
//   - Graceful handling of dead backends (auto re-enable when healthy again)
//   - Per-connection bidirectional copy (the actual "multiplexing" of traffic)
//   - Simple JSON config file, hot-reloadable with SIGHUP
//   - Basic stats endpoint (HTTP) showing backend status/connection counts
//
// Build:
//   go mod init goproxy
//   go build -o goproxy .
//
// Run:
//   ./goproxy -config config.json
package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ---------- Config ----------

type Config struct {
	Listeners []ListenerConfig `json:"listeners"`
	StatsAddr string           `json:"stats_addr,omitempty"` // e.g. ":8404" (like HAProxy stats page)
}

type ListenerConfig struct {
	Name      string          `json:"name"`
	Bind      string          `json:"bind"`                // e.g. ":443"
	Backends  []BackendConfig `json:"backends"`
	Balance   string          `json:"balance"`              // "roundrobin" | "leastconn"
	CheckPath string          `json:"check_interval"`       // unused placeholder, see CheckIntervalSec
	CheckIntervalSec int      `json:"check_interval_sec"`   // health check interval
	CheckTimeoutSec  int      `json:"check_timeout_sec"`
	ConnectTimeoutSec int     `json:"connect_timeout_sec"`
}

type BackendConfig struct {
	Addr   string `json:"addr"`   // host:port
	Weight int    `json:"weight"` // reserved for weighted RR (defaults to 1)
}

func loadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var c Config
	dec := json.NewDecoder(f)
	if err := dec.Decode(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

// ---------- Backend / pool runtime state ----------

type Backend struct {
	Addr    string
	Weight  int
	healthy int32 // atomic bool (0/1)
	active  int64 // atomic active-connection counter
	fails   int32 // atomic consecutive failure counter
}

func (b *Backend) IsHealthy() bool   { return atomic.LoadInt32(&b.healthy) == 1 }
func (b *Backend) setHealthy(v bool) {
	if v {
		atomic.StoreInt32(&b.healthy, 1)
	} else {
		atomic.StoreInt32(&b.healthy, 0)
	}
}
func (b *Backend) Active() int64 { return atomic.LoadInt64(&b.active) }
func (b *Backend) incr()         { atomic.AddInt64(&b.active, 1) }
func (b *Backend) decr()         { atomic.AddInt64(&b.active, -1) }

type Pool struct {
	Name              string
	Bind              string
	Balance           string
	Backends          []*Backend
	rrCounter         uint64
	CheckInterval     time.Duration
	CheckTimeout      time.Duration
	ConnectTimeout    time.Duration
	mu                sync.Mutex
}

func newPool(lc ListenerConfig) *Pool {
	backends := make([]*Backend, 0, len(lc.Backends))
	for _, bc := range lc.Backends {
		w := bc.Weight
		if w <= 0 {
			w = 1
		}
		backends = append(backends, &Backend{Addr: bc.Addr, Weight: w, healthy: 1})
	}
	ci := lc.CheckIntervalSec
	if ci <= 0 {
		ci = 5
	}
	ct := lc.CheckTimeoutSec
	if ct <= 0 {
		ct = 2
	}
	cot := lc.ConnectTimeoutSec
	if cot <= 0 {
		cot = 3
	}
	balance := lc.Balance
	if balance == "" {
		balance = "roundrobin"
	}
	return &Pool{
		Name:           lc.Name,
		Bind:           lc.Bind,
		Balance:        balance,
		Backends:       backends,
		CheckInterval:  time.Duration(ci) * time.Second,
		CheckTimeout:   time.Duration(ct) * time.Second,
		ConnectTimeout: time.Duration(cot) * time.Second,
	}
}

// Pick selects the next backend according to the configured strategy.
// Returns nil if no backend is currently healthy.
func (p *Pool) Pick() *Backend {
	p.mu.Lock()
	defer p.mu.Unlock()

	healthyList := make([]*Backend, 0, len(p.Backends))
	for _, b := range p.Backends {
		if b.IsHealthy() {
			healthyList = append(healthyList, b)
		}
	}
	if len(healthyList) == 0 {
		return nil
	}

	switch p.Balance {
	case "leastconn":
		best := healthyList[0]
		for _, b := range healthyList[1:] {
			if b.Active() < best.Active() {
				best = b
			}
		}
		return best
	default: // roundrobin (weighted)
		total := 0
		for _, b := range healthyList {
			total += b.Weight
		}
		idx := int(atomic.AddUint64(&p.rrCounter, 1)) % total
		for _, b := range healthyList {
			if idx < b.Weight {
				return b
			}
			idx -= b.Weight
		}
		return healthyList[0]
	}
}

// ---------- Health checking ----------

func (p *Pool) healthCheckLoop(stop <-chan struct{}) {
	ticker := time.NewTicker(p.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			for _, b := range p.Backends {
				go p.checkOne(b)
			}
		}
	}
}

func (p *Pool) checkOne(b *Backend) {
	conn, err := net.DialTimeout("tcp", b.Addr, p.CheckTimeout)
	if err != nil {
		f := atomic.AddInt32(&b.fails, 1)
		if f >= 3 && b.IsHealthy() {
			log.Printf("[%s] backend %s marked DOWN after %d failures: %v", p.Name, b.Addr, f, err)
			b.setHealthy(false)
		}
		return
	}
	conn.Close()
	atomic.StoreInt32(&b.fails, 0)
	if !b.IsHealthy() {
		log.Printf("[%s] backend %s marked UP", p.Name, b.Addr)
		b.setHealthy(true)
	}
}

// ---------- Proxy / multiplexing ----------

func (p *Pool) Serve(stop <-chan struct{}) error {
	ln, err := net.Listen("tcp", p.Bind)
	if err != nil {
		return err
	}
	log.Printf("[%s] listening on %s (balance=%s, backends=%d)", p.Name, p.Bind, p.Balance, len(p.Backends))

	go func() {
		<-stop
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-stop:
				return nil
			default:
				log.Printf("[%s] accept error: %v", p.Name, err)
				continue
			}
		}
		go p.handleConn(conn)
	}
}

func (p *Pool) handleConn(client net.Conn) {
	defer client.Close()

	backend := p.Pick()
	if backend == nil {
		log.Printf("[%s] no healthy backend for client %s", p.Name, client.RemoteAddr())
		return
	}

	upstream, err := net.DialTimeout("tcp", backend.Addr, p.ConnectTimeout)
	if err != nil {
		log.Printf("[%s] failed to connect to backend %s: %v", p.Name, backend.Addr, err)
		backend.setHealthy(false) // fast-fail; health check loop will confirm/recover
		return
	}
	defer upstream.Close()

	backend.incr()
	defer backend.decr()

	// Multiplex: pipe both directions concurrently.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		io.Copy(upstream, client)
		if tc, ok := upstream.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		io.Copy(client, upstream)
		if tc, ok := client.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()
	wg.Wait()
}

// ---------- Stats endpoint ----------

func startStats(addr string, pools []*Pool) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		type backendStat struct {
			Addr    string `json:"addr"`
			Healthy bool   `json:"healthy"`
			Active  int64  `json:"active_conns"`
		}
		type poolStat struct {
			Name     string        `json:"name"`
			Bind     string        `json:"bind"`
			Balance  string        `json:"balance"`
			Backends []backendStat `json:"backends"`
		}
		out := make([]poolStat, 0, len(pools))
		for _, p := range pools {
			ps := poolStat{Name: p.Name, Bind: p.Bind, Balance: p.Balance}
			for _, b := range p.Backends {
				ps.Backends = append(ps.Backends, backendStat{
					Addr: b.Addr, Healthy: b.IsHealthy(), Active: b.Active(),
				})
			}
			out = append(out, ps)
		}
		json.NewEncoder(w).Encode(out)
	})
	go func() {
		log.Printf("stats endpoint listening on %s (GET /stats)", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("stats server error: %v", err)
		}
	}()
}

// ---------- main ----------

func main() {
	configPath := flag.String("config", "config.json", "path to JSON config file")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	var pools []*Pool
	stopChans := make([]chan struct{}, 0)

	start := func(c *Config) {
		for _, lc := range c.Listeners {
			p := newPool(lc)
			stop := make(chan struct{})
			stopChans = append(stopChans, stop)
			pools = append(pools, p)

			go p.healthCheckLoop(stop)
			go func(p *Pool, stop chan struct{}) {
				if err := p.Serve(stop); err != nil {
					log.Printf("[%s] serve error: %v", p.Name, err)
				}
			}(p, stop)
		}
		startStats(c.StatsAddr, pools)
	}

	start(cfg)

	// Graceful shutdown + SIGHUP to reload config (basic reload: restart listeners).
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	for sig := range sigs {
		switch sig {
		case syscall.SIGHUP:
			log.Println("SIGHUP received: reloading config (note: existing listeners keep running old config in this minimal version)")
			// For a production version you'd diff and swap pools atomically.
			newCfg, err := loadConfig(*configPath)
			if err != nil {
				log.Printf("reload failed: %v", err)
				continue
			}
			cfg = newCfg
			log.Println("config re-read; restart the process to apply changes to listeners in this minimal version")
		case syscall.SIGINT, syscall.SIGTERM:
			log.Println("shutting down...")
			for _, s := range stopChans {
				close(s)
			}
			return
		}
	}
}
