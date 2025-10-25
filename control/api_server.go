package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// DeployRequest : JSON fallback (not used when uploading multipart)
type DeployRequest struct {
	User   string `json:"user"`
	Site   string `json:"site"`
	Folder string `json:"folder"` // optional
}

// startHTTPServer runs a REST API on the node
// It also serves uploaded zips under /uploads/
func (n *RaftNode) startHTTPServer(port int) {
	mux := http.NewServeMux()

	uploadsDir := "uploads"
	if err := os.MkdirAll(uploadsDir, 0o755); err != nil {
		log.Printf("[%s] cannot create uploads dir: %v\n", n.ID, err)
	}

	// serve uploads under /uploads/<nodeID>/...
	mux.Handle("/uploads/", http.StripPrefix("/uploads/", http.FileServer(http.Dir(uploadsDir))))

	// health/status
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		n.mu.Lock()
		resp := struct {
			ID       string                `json:"id"`
			Role     string                `json:"role"`
			Term     int                   `json:"term"`
			LeaderID string                `json:"leader"`
			Workers  map[string]WorkerInfo `json:"workers"`
		}{
			ID:       n.ID,
			Role:     string(n.role),
			Term:     n.persistent.CurrentTerm,
			LeaderID: n.leaderID,
			Workers:  n.Workers,
		}
		n.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	// deploy: accepts multipart/form-data with fields: user, site, file (zip)
	_ = os.MkdirAll(uploadsDir, 0o755)

	mux.HandleFunc("/deploy", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// parse multipart form (50MB max)
		if err := r.ParseMultipartForm(50 << 20); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		user := r.FormValue("user")
		site := r.FormValue("site")
		if user == "" || site == "" {
			http.Error(w, "missing user or site", http.StatusBadRequest)
			return
		}

		file, header, err := r.FormFile("file")
		if err != nil {
			http.Error(w, "missing file: "+err.Error(), http.StatusBadRequest)
			return
		}
		defer file.Close()

		// Save uploaded file to uploads/<nodeID>/<timestamp>_<filename>
		nodeDir := filepath.Join(uploadsDir, n.ID)
		if err := os.MkdirAll(nodeDir, 0o755); err != nil {
			http.Error(w, "cannot create node upload dir: "+err.Error(), http.StatusInternalServerError)
			return
		}

		ts := time.Now().UnixNano()
		fileName := fmt.Sprintf("%d_%s", ts, filepath.Base(header.Filename))
		destPath := filepath.Join(nodeDir, fileName)

		out, err := os.Create(destPath)
		if err != nil {
			http.Error(w, "cannot create file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := io.Copy(out, file); err != nil {
			out.Close()
			http.Error(w, "cannot save file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		_ = out.Close()

		// Build accessible URL for the cluster to download.
		// Use Host header if present (it includes host:port), fallback to advertised host and API port.
		host := r.Host
		if host == "" {
			// The HTTP API listens on RPC port + 100
			host = fmt.Sprintf("%s:%d", n.Host, port+100)
		}
		fileURL := fmt.Sprintf("http://%s/uploads/%s/%s", host, n.ID, fileName)
		log.Printf("[%s] Received site upload: user=%s site=%s file=%s url=%s\n", n.ID, user, site, destPath, fileURL)

		// Ask leader to deploy (append deploy command to Raft log)
		if err := n.DeploySiteWithURL(user, site, fileURL); err != nil {
			log.Printf("[%s] DeploySiteWithURL failed: %v\n", n.ID, err)
			http.Error(w, "deploy failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		fmt.Fprintf(w, "Deployment requested. user=%s site=%s\n", user, site)
	})

	// Start REST API server on port+100
	addr := fmt.Sprintf(":%d", port+100)
	log.Printf("[%s] REST API listening on %s\n", n.ID, addr)
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[%s] REST server failed: %v", n.ID, err)
		}
	}()
}
