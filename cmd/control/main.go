package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Kevalj9999/IAAS-cluster/pkg/config"
	"github.com/Kevalj9999/IAAS-cluster/pkg/deployer"
	"github.com/Kevalj9999/IAAS-cluster/pkg/heartbeat"
	"github.com/Kevalj9999/IAAS-cluster/pkg/httpserver"
	rpkg "github.com/Kevalj9999/IAAS-cluster/pkg/raft"
)

func main() {
	cfg := config.ParseControlFlags()
	if err := os.MkdirAll(cfg.BundleDir, 0o755); err != nil {
		log.Fatalf("mkdir bundle dir: %v", err)
	}

	raftAddr := getEnvOrDefault("RAFT_ADDR", ":12000")
	nodeID := getEnvOrDefault("NODE_ID", "node1")

	var raftPeers []string
	if peersEnv := os.Getenv("RAFT_PEERS"); peersEnv != "" {
		for _, p := range strings.Split(peersEnv, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				raftPeers = append(raftPeers, p)
			}
		}
	}

	dataDir := filepath.Join(cfg.BundleDir, "raft-"+nodeID)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Fatalf("mkdir raft data dir: %v", err)
	}
	raftNode, err := rpkg.NewRaftNode(dataDir, raftAddr, nodeID, raftPeers)
	if err != nil {
		log.Fatalf("failed to start raft: %v", err)
	}
	defer raftNode.Shutdown()

	monitor := heartbeat.NewMonitor(cfg.WorkerAddrs, cfg.HeartbeatInt)
	monitor.Start()
	defer monitor.Stop()

	mux := http.NewServeMux()

	mux.HandleFunc("/deploy", func(w http.ResponseWriter, r *http.Request) {
		if !raftNode.IsLeader() {
			ldr := raftNode.Leader()
			if ldr == "" {
				http.Error(w, "no leader elected", 503)
				return
			}
			redirectURL := fmt.Sprintf("http://%s/deploy", ldr)
			http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
			return
		}

		if r.Method != "POST" {
			http.Error(w, "only POST allowed", http.StatusMethodNotAllowed)
			return
		}
		file, header, err := r.FormFile("site")
		if err != nil {
			http.Error(w, "missing file: "+err.Error(), 400)
			return
		}
		defer file.Close()

		dstPath := filepath.Join(cfg.BundleDir, fmt.Sprintf("%d-%s", time.Now().UnixNano(), header.Filename))
		out, err := os.Create(dstPath)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		if _, err := io.Copy(out, file); err != nil {
			out.Close()
			http.Error(w, err.Error(), 500)
			return
		}
		out.Close()

		cmdObj := map[string]interface{}{
			"filename": filepath.Base(dstPath),
			"ts":       time.Now().Unix(),
		}
		cmdBytes, _ := json.Marshal(cmdObj)

		if err := raftNode.Propose(cmdBytes, 10*time.Second); err != nil {
			http.Error(w, "raft propose error: "+err.Error(), 500)
			return
		}

		healthy := monitor.Healthy()
		if len(healthy) == 0 {
			http.Error(w, "no healthy workers", 503)
			return
		}
		worker := healthy[0]

		go func() {
			if err := deployer.DeployToWorker(worker, dstPath); err != nil {
				log.Printf("deploy to %s failed: %v", worker, err)
			} else {
				log.Printf("deployed %s -> %s", dstPath, worker)
			}
		}()

		fmt.Fprintf(w, "replicated and deployed to %s", worker)
	})

	mux.HandleFunc("/workers", func(w http.ResponseWriter, r *http.Request) {
		healthy := monitor.Healthy()
		fmt.Fprintf(w, "leader: %s\nhealthy: %v\nall: %v\n", raftNode.Leader(), healthy, cfg.WorkerAddrs)
	})

	mux.HandleFunc("/health", httpserver.MakeHealthHandler())

	addr := ":" + strconv.Itoa(cfg.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 20 * time.Second,
	}
	log.Printf("control serving on %s (raft addr=%s node=%s)", addr, raftAddr, nodeID)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("listen: %v", err)
	}
}

func getEnvOrDefault(k, d string) string {
	v := os.Getenv(k)
	if v == "" {
		return d
	}
	return v
}
