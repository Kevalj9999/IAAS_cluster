package config

import (
	"flag"
	"strings"
)

// ControlConfig holds control node config
type ControlConfig struct {
	Port         int
	WorkerAddrs  []string
	BundleDir    string
	HeartbeatInt int // seconds
}

// WorkerConfig holds worker node config
type WorkerConfig struct {
	Port              int
	WorkDir           string
	WebRoot           string
	HeartbeatInterval int // seconds (for optional reverse heartbeats)
}

// ParseControlFlags returns config from flags
func ParseControlFlags() ControlConfig {
	var port int
	var workers string
	var bundleDir string
	var hb int
	flag.IntVar(&port, "port", 8080, "control node HTTP port")
	flag.StringVar(&workers, "workers", "http://localhost:9001", "comma-separated worker addresses")
	flag.StringVar(&bundleDir, "bundledir", "./bundles", "directory to save uploaded bundles")
	flag.IntVar(&hb, "heartbeat", 5, "heartbeat interval seconds")
	flag.Parse()
	return ControlConfig{
		Port:         port,
		WorkerAddrs:  splitAndTrim(workers),
		BundleDir:    bundleDir,
		HeartbeatInt: hb,
	}
}

// ParseWorkerFlags returns worker config
func ParseWorkerFlags() WorkerConfig {
	var port int
	var workdir string
	var webroot string
	var hb int
	flag.IntVar(&port, "port", 9001, "worker HTTP port")
	flag.StringVar(&workdir, "workdir", "/tmp/worker", "worker working directory")
	flag.StringVar(&webroot, "webroot", "www", "subdirectory under workdir to serve files from")
	flag.IntVar(&hb, "heartbeat", 0, "if >0, send heartbeats to control every N seconds")
	flag.Parse()
	return WorkerConfig{
		Port:              port,
		WorkDir:           workdir,
		WebRoot:           webroot,
		HeartbeatInterval: hb,
	}
}

func splitAndTrim(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
