package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/Kevalj9999/IAAS-cluster/pkg/config"
	"github.com/Kevalj9999/IAAS-cluster/pkg/httpserver"
)

func getEnvOrDefault(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}

func main() {
	cfg := config.ParseWorkerFlags()
	// Ensure workdir and webroot exist
	if err := os.MkdirAll(cfg.WorkDir, 0o755); err != nil {
		log.Fatalf("mkdir workdir: %v", err)
	}
	webrootPath := fmt.Sprintf("%s/%s", cfg.WorkDir, cfg.WebRoot)
	_ = os.MkdirAll(webrootPath, 0o755)

	mux := http.NewServeMux()
	mux.HandleFunc("/deploy", httpserver.MakeDeployHandler(cfg.WorkDir, cfg.WebRoot))
	mux.HandleFunc("/health", httpserver.MakeHealthHandler())
	mux.Handle("/", httpserver.MakeStaticFileServer(cfg.WorkDir, cfg.WebRoot))

	addr := ":" + getEnvOrDefault("PORT", "9001")
	log.Printf("worker serving on %s (webroot=%s)", addr, webrootPath)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("listen: %v", err)
	}
}
