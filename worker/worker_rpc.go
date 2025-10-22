package main

import (
	"archive/zip"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// AssignDeploymentArgs instructs the worker to download & serve a static site
type AssignDeploymentArgs struct {
	User    string
	Site    string
	FileURL string // http(s) URL to zip file
}

// AssignDeploymentReply is the RPC response
type AssignDeploymentReply struct {
	Success bool
	Message string
}

// WorkerRPC implements the RPC methods for deployment
type WorkerRPC struct {
	BasePath string // where sites are stored, e.g., "./sites"
}

// ====================
// Utility functions
// ====================

// downloadToTemp downloads a given URL to a temporary local zip file.
// It retries a few times and validates HTTP status code and file size.
func downloadToTemp(url string) (string, error) {
	const maxAttempts = 3
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		resp, err := http.Get(url)
		if err != nil {
			lastErr = fmt.Errorf("attempt %d: http.Get error: %v", attempt, err)
			log.Printf("[worker] download attempt %d failed: %v\n", attempt, err)
			time.Sleep(300 * time.Millisecond)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			//bodySnippet := ""
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("attempt %d: bad status %d", attempt, resp.StatusCode)
			log.Printf("[worker] download attempt %d returned status %d for %s\n", attempt, resp.StatusCode, url)
			time.Sleep(300 * time.Millisecond)
			continue
		}

		tmpFile, err := os.CreateTemp("", "site_*.zip")
		if err != nil {
			resp.Body.Close()
			return "", fmt.Errorf("failed to create temp file: %v", err)
		}

		n, err := io.Copy(tmpFile, resp.Body)
		resp.Body.Close()
		if err != nil {
			tmpFile.Close()
			os.Remove(tmpFile.Name())
			lastErr = fmt.Errorf("attempt %d: copy error: %v", attempt, err)
			log.Printf("[worker] download attempt %d copy failed: %v\n", attempt, err)
			time.Sleep(300 * time.Millisecond)
			continue
		}
		tmpFile.Close()

		if n == 0 {
			os.Remove(tmpFile.Name())
			lastErr = fmt.Errorf("attempt %d: zero-length file downloaded", attempt)
			log.Printf("[worker] download attempt %d got zero-length file\n", attempt)
			time.Sleep(300 * time.Millisecond)
			continue
		}

		log.Printf("[worker] downloaded %d bytes to %s\n", n, tmpFile.Name())
		return tmpFile.Name(), nil
	}

	return "", fmt.Errorf("download failed after %d attempts: %v", 3, lastErr)
}

// unzip extracts a zip file to the specified destination folder
func unzip(src, destDir string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return fmt.Errorf("open zip error: %v", err)
	}
	defer r.Close()

	for _, f := range r.File {
		// Prevent ZipSlip vulnerability
		fpath := filepath.Join(destDir, f.Name)
		if !strings.HasPrefix(fpath, filepath.Clean(destDir)+string(os.PathSeparator)) {
			return fmt.Errorf("invalid file path in zip: %s", fpath)
		}

		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(fpath, os.ModePerm); err != nil {
				return err
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return err
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			outFile.Close()
			return err
		}
		_, err = io.Copy(outFile, rc)
		outFile.Close()
		rc.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

// listFiles logs files in the target directory (useful to verify extraction)
func listFiles(dir string, max int) {
	i := 0
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		rel, _ := filepath.Rel(dir, path)
		if rel == "." {
			return nil
		}
		log.Printf("[worker] site file: %s (dir=%t)\n", rel, info.IsDir())
		i++
		if i >= max {
			return fmt.Errorf("limit reached")
		}
		return nil
	})
}

// ====================
// RPC methods
// ====================

// AssignDeployment downloads, extracts, and prepares the static site
func (w *WorkerRPC) AssignDeployment(args AssignDeploymentArgs, reply *AssignDeploymentReply) error {
	base := w.BasePath
	if base == "" {
		base = "./sites"
	}

	// ensure sanitize user/site (basic)
	user := filepath.Clean(args.User)
	site := filepath.Clean(args.Site)
	if user == "." || site == "." || strings.Contains(user, "..") || strings.Contains(site, "..") {
		reply.Success = false
		reply.Message = "invalid user or site name"
		return nil
	}

	targetDir := filepath.Join(base, user, site)
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		reply.Success = false
		reply.Message = fmt.Sprintf("mkdir failed: %v", err)
		return nil
	}

	log.Printf("[worker] AssignDeployment: downloading %s for %s/%s -> %s\n", args.FileURL, user, site, targetDir)
	tmpZip, err := downloadToTemp(args.FileURL)
	if err != nil {
		reply.Success = false
		reply.Message = fmt.Sprintf("download failed: %v", err)
		log.Printf("[worker] ERROR download: %v\n", err)
		return nil
	}
	// ensure temp file removed
	defer func() {
		if tmpZip != "" {
			os.Remove(tmpZip)
		}
	}()

	// unzip into a temporary directory first, then move into targetDir to avoid partial deployments
	tmpExtractDir, err := os.MkdirTemp("", "site_extract_*")
	if err != nil {
		reply.Success = false
		reply.Message = fmt.Sprintf("temp extract dir creation failed: %v", err)
		log.Printf("[worker] ERROR tmp dir: %v\n", err)
		return nil
	}
	// cleanup temp extract on exit
	defer func() {
		_ = os.RemoveAll(tmpExtractDir)
	}()

	if err := unzip(tmpZip, tmpExtractDir); err != nil {
		reply.Success = false
		reply.Message = fmt.Sprintf("unzip failed: %v", err)
		log.Printf("[worker] ERROR unzip: %v\n", err)
		return nil
	}

	// Optional: remove old targetDir contents before moving new (atomic replace)
	backupDir := targetDir + ".old"
	_ = os.RemoveAll(backupDir)
	// move current to backup
	_ = os.Rename(targetDir, backupDir)
	// create fresh target
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		// Attempt to roll back
		_ = os.Rename(backupDir, targetDir)
		reply.Success = false
		reply.Message = fmt.Sprintf("mkdir target failed: %v", err)
		log.Printf("[worker] ERROR mkdir target: %v\n", err)
		return nil
	}

	// move extracted files into targetDir
	err = filepath.Walk(tmpExtractDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(tmpExtractDir, path)
		if err != nil {
			return err
		}
		destPath := filepath.Join(targetDir, rel)
		if info.IsDir() {
			return os.MkdirAll(destPath, os.ModePerm)
		}
		// ensure parent exists
		if err := os.MkdirAll(filepath.Dir(destPath), os.ModePerm); err != nil {
			return err
		}
		// move (rename) where possible, otherwise copy
		if err := os.Rename(path, destPath); err == nil {
			return nil
		}
		// fallback to copy
		src, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src.Close()
		dst, err := os.OpenFile(destPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
		if err != nil {
			return err
		}
		defer dst.Close()
		_, err = io.Copy(dst, src)
		return err
	})
	if err != nil {
		// rollback
		_ = os.RemoveAll(targetDir)
		_ = os.Rename(backupDir, targetDir)
		reply.Success = false
		reply.Message = fmt.Sprintf("deploy move error: %v", err)
		log.Printf("[worker] ERROR move extracted files: %v\n", err)
		return nil
	}

	// remove backup on successful replace
	_ = os.RemoveAll(backupDir)

	// log contents so operator can verify
	log.Printf("[worker] Assigned site %s/%s -> %s (listing up to 100 files):\n", user, site, targetDir)
	listFiles(targetDir, 100)

	reply.Success = true
	reply.Message = "deployed successfully"
	log.Printf("[worker] Deployment finished for %s/%s\n", user, site)
	return nil
}

// ====================
// Worker startup
// ====================

// startWorkerRPC runs both RPC and HTTP servers for the worker
func startWorkerRPC(baseDir string, httpPort int) {
	// Ensure baseDir exists
	if baseDir == "" {
		baseDir = "./sites"
	}
	_ = os.MkdirAll(baseDir, 0o755)

	// 1️⃣ Start RPC server (on httpPort + 1000)
	rpcSrv := rpc.NewServer()
	if err := rpcSrv.Register(&WorkerRPC{BasePath: baseDir}); err != nil {
		log.Fatalf("WorkerRPC register error: %v", err)
	}

	rpcPort := httpPort + 1000
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", rpcPort))
	if err != nil {
		log.Fatalf("WorkerRPC listen error: %v", err)
	}
	log.Printf("[worker] RPC listening on %d\n", rpcPort)
	go rpcSrv.Accept(l)

	// 2️⃣ Serve static files over HTTP using a local mux (avoid global http.Handle)
	mux := http.NewServeMux()
	fileHandler := http.FileServer(http.Dir(baseDir))
	mux.Handle("/", fileHandler)

	addr := fmt.Sprintf(":%d", httpPort)
	log.Printf("[worker] Serving HTTP on %s (baseDir=%s)\n", addr, baseDir)
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()
}
