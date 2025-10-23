package deployer

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// DeployToWorker sends bundle bytes to worker's /deploy endpoint.
func DeployToWorker(workerAddr string, bundlePath string) error {
	file, err := os.Open(bundlePath)
	if err != nil {
		return err
	}
	defer file.Close()
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("site", filepath.Base(bundlePath))
	if err != nil {
		return err
	}
	if _, err := io.Copy(part, file); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	client := &http.Client{Timeout: 30 * time.Second}
	url := fmt.Sprintf("%s/deploy", workerAddr)
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", writer.FormDataContentType())
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("deploy failed: %s", string(respBody))
	}
	return nil
}
