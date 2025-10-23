package storage

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// SaveBundle saves an io.Reader (bundle) to a file path. Caller is responsible for opening the reader.
func SaveBundle(destDir, filename string, r io.Reader) (string, error) {
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return "", err
	}
	destPath := filepath.Join(destDir, filename)
	f, err := os.Create(destPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	n, err := io.Copy(f, r)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s (%d bytes)", destPath, n), nil
}

// ExtractTarGz extracts a tar.gz from reader into destination dir.
func ExtractTarGz(r io.Reader, destDir string) error {
	gzr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gzr.Close()
	tr := tar.NewReader(gzr)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		target := filepath.Join(destDir, hdr.Name)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode)); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			outFile, err := os.Create(target)
			if err != nil {
				return err
			}
			if _, err := io.Copy(outFile, tr); err != nil {
				outFile.Close()
				return err
			}
			outFile.Close()
			if err := os.Chmod(target, os.FileMode(hdr.Mode)); err != nil {
				return err
			}
		default:
			// ignore other types for simplicity
		}
	}
	return nil
}
