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

// startHTTPServer runs a REST API on the node.
// It serves both /uploads/ (shared) and /sites/ (deployed websites).
func (n *RaftNode) startHTTPServer(port int) {
	mux := http.NewServeMux()

	uploadsDir := "uploads"
	if err := os.MkdirAll(uploadsDir, 0o755); err != nil {
		log.Printf("[%s] cannot create uploads dir: %v\n", n.ID, err)
	}

	// ✅ Serve shared uploads
	mux.Handle("/uploads/", http.StripPrefix("/uploads/", http.FileServer(http.Dir(uploadsDir))))

	// ✅ Serve deployed sites from this node’s sites directory
	sitesRoot := n.SitesDir
	if err := os.MkdirAll(sitesRoot, 0o755); err != nil {
		log.Printf("[%s] cannot create sites dir: %v\n", n.ID, err)
	}
	mux.Handle("/sites/", http.StripPrefix("/sites/", http.FileServer(http.Dir(sitesRoot))))

	// Health/status endpoint
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

	// ✅ Deploy endpoint
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

		// Save uploaded file to shared_uploads
		ts := time.Now().UnixNano()
		fileName := fmt.Sprintf("%d_%s", ts, filepath.Base(header.Filename))
		destPath := filepath.Join(uploadsDir, fileName)

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

		// Construct shared URL (served by any node)
		host := r.Host
		if host == "" {
			host = fmt.Sprintf("%s:%d", n.Host, port+100)
		}
		fileURL := fmt.Sprintf("http://%s/uploads/%s", host, fileName)

		log.Printf("[%s] Received site upload: user=%s site=%s file=%s url=%s\n",
			n.ID, user, site, destPath, fileURL)

		if err := n.DeploySiteWithURL(user, site, fileURL); err != nil {
			log.Printf("[%s] DeploySiteWithURL failed: %v\n", n.ID, err)
			http.Error(w, "deploy failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		fmt.Fprintf(w, "Deployment requested. user=%s site=%s\n", user, site)
	})

	// ✅ Start REST API server
	addr := fmt.Sprintf(":%d", port+100)
	log.Printf("[%s] REST API listening on %s\n", n.ID, addr)
	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[%s] REST server failed: %v", n.ID, err)
		}
	}()
}
