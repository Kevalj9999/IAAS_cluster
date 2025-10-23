package heartbeat

import (
	"net/http"
	"sync"
	"time"
)

// HeartbeatMonitor pings workers and tracks health
type HeartbeatMonitor struct {
	Workers  []string
	Interval time.Duration
	health   map[string]bool
	mu       sync.RWMutex
	stop     chan struct{}
}

// NewMonitor creates a monitor
func NewMonitor(workers []string, intervalSeconds int) *HeartbeatMonitor {
	h := &HeartbeatMonitor{
		Workers:  workers,
		Interval: time.Duration(intervalSeconds) * time.Second,
		health:   map[string]bool{},
		stop:     make(chan struct{}),
	}
	for _, w := range workers {
		h.health[w] = false
	}
	return h
}

// Start begins periodic pings
func (h *HeartbeatMonitor) Start() {
	go func() {
		t := time.NewTicker(h.Interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				h.pingAll()
			case <-h.stop:
				return
			}
		}
	}()
}

func (h *HeartbeatMonitor) Stop() {
	close(h.stop)
}

func (h *HeartbeatMonitor) pingAll() {
	for _, w := range h.Workers {
		go h.ping(w)
	}
}

func (h *HeartbeatMonitor) ping(worker string) {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(worker + "/health")
	ok := err == nil && resp.StatusCode == 200
	if resp != nil {
		resp.Body.Close()
	}
	h.mu.Lock()
	h.health[worker] = ok
	h.mu.Unlock()
}

// Healthy returns list of healthy workers
func (h *HeartbeatMonitor) Healthy() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var out []string
	for w, ok := range h.health {
		if ok {
			out = append(out, w)
		}
	}
	return out
}
