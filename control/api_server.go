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

// startHTTPServer runs a REST API on the control node
// It also serves uploaded zips under /uploads/
func (n *RaftNode) startHTTPServer(port int) {
	mux := http.NewServeMux()

	// ensure uploads dir
	uploadsDir := filepath.Join("uploads", n.ID)
	if err := os.MkdirAll(uploadsDir, 0o755); err != nil {
		log.Printf("[%s] cannot create uploads dir: %v\n", n.ID, err)
	}

	// serve uploads under /uploads/<nodeID>/...
	mux.Handle("/uploads/", http.StripPrefix("/uploads/", http.FileServer(http.Dir("uploads"))))

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
	mux.HandleFunc("/deploy", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// parse multipart
		err := r.ParseMultipartForm(50 << 20) // 50MB max
		if err != nil {
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

		// save to uploads/<nodeID>/<timestamp>_<filename>
		ts := time.Now().UnixNano()
		fileName := fmt.Sprintf("%d_%s", ts, filepath.Base(header.Filename))
		destDir := uploadsDir
		destPath := filepath.Join(destDir, fileName)
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
		out.Close()

		// Build accessible URL for the worker to download
		// Use Host header if present to build absolute URL, otherwise use localhost and rest port (port+100)
		host := r.Host
		if host == "" {
			host = fmt.Sprintf("localhost:%d", port+100)
		}
		// public URL: http://<host>/uploads/<nodeID>/<fileName>
		fileURL := fmt.Sprintf("http://%s/uploads/%s/%s", host, n.ID, fileName)

		log.Printf("[%s] Received site upload: user=%s site=%s file=%s url=%s\n", n.ID, user, site, destPath, fileURL)

		// ask leader to deploy (this will append deploy command to Raft log)
		err2 := n.DeploySiteWithURL(user, site, fileURL)
		if err2 != nil {
			log.Printf("[%s] DeploySiteWithURL failed: %v\n", n.ID, err2)
			http.Error(w, "deploy failed: "+err2.Error(), http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, "Deployment requested. user=%s site=%s\n", user, site)
	})

	addr := fmt.Sprintf(":%d", port+100) // REST port offset (+100)
	log.Printf("[%s] REST API listening on %s\n", n.ID, addr)
	srv := &http.Server{Addr: addr, Handler: mux}
	go srv.ListenAndServe()
}
