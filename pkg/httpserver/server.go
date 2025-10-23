package httpserver

import (
	"archive/zip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

// Simple handler to accept bundle uploads and store to destDir, returns saved path
func MakeUploadHandler(destDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "only POST", http.StatusMethodNotAllowed)
			return
		}
		file, header, err := r.FormFile("site")
		if err != nil {
			http.Error(w, "missing file: "+err.Error(), http.StatusBadRequest)
			return
		}
		defer file.Close()

		savePath := filepath.Join(destDir, header.Filename)
		if err := os.MkdirAll(destDir, 0o755); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		out, err := os.Create(savePath)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer out.Close()
		if _, err := io.Copy(out, file); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		fmt.Fprintf(w, "saved:%s", savePath)
	}
}

// MakeDeployHandler is for workers: accepts uploaded tar.gz and extracts into webroot
func MakeDeployHandler(workdir, webroot string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "only POST allowed", http.StatusMethodNotAllowed)
			return
		}

		file, header, err := r.FormFile("site")
		if err != nil {
			http.Error(w, "file missing: "+err.Error(), http.StatusBadRequest)
			return
		}
		defer file.Close()

		targetDir := filepath.Join(workdir, webroot)
		if err := os.MkdirAll(targetDir, 0o755); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		tempZip := filepath.Join(workdir, header.Filename)
		out, err := os.Create(tempZip)
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

		if err := extractZip(tempZip, targetDir); err != nil {
			http.Error(w, "extract error: "+err.Error(), 500)
			return
		}

		fmt.Fprintf(w, "deployed ZIP to %s", targetDir)
	}
}

// extractZip extracts a .zip file to destDir
func extractZip(zipPath, destDir string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		fpath := filepath.Join(destDir, f.Name)
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(fpath, f.Mode()); err != nil {
				return err
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(fpath), 0o755); err != nil {
			return err
		}

		srcFile, err := f.Open()
		if err != nil {
			return err
		}
		defer srcFile.Close()

		dstFile, err := os.Create(fpath)
		if err != nil {
			return err
		}
		if _, err := io.Copy(dstFile, srcFile); err != nil {
			dstFile.Close()
			return err
		}
		dstFile.Close()
	}
	return nil
}

func MakeHealthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("OK"))
	}
}

func MakeStaticFileServer(workdir, webroot string) http.Handler {
	target := filepath.Join(workdir, webroot)
	fs := http.FileServer(http.Dir(target))
	return http.StripPrefix("/", fs)
}
