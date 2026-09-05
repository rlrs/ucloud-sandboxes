package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// These operations run inside the guest under the exec identity. They are not
// a host filesystem broker or an authorization boundary against guest root.
func runFiles(args []string, input io.Reader, output io.Writer) error {
	if len(args) == 1 && args[0] == "ready" {
		return nil
	}
	if len(args) != 3 {
		return errors.New("usage: files read|write absolute-path max-bytes")
	}
	operation, path := args[0], args[1]
	if operation != "read" && operation != "write" {
		return errors.New("unsupported file operation")
	}
	if !filepath.IsAbs(path) {
		return errors.New("file path must be absolute")
	}
	for _, part := range strings.Split(path, "/") {
		if part == ".." {
			return errors.New("parent traversal is unsupported")
		}
	}
	for _, char := range path {
		if char < 32 || char == 127 {
			return errors.New("file path contains control characters")
		}
	}
	limit, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil || limit < 1 || limit > 256*1024*1024 {
		return errors.New("file limit must be 1..268435456 bytes")
	}
	if operation == "read" {
		file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
		if err != nil {
			return err
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("file read requires a regular file")
		}
		if info.Size() > limit {
			return errors.New("file exceeds byte limit")
		}
		if _, err = io.Copy(output, io.LimitReader(file, limit)); err != nil {
			return err
		}
		var extra [1]byte
		if n, err := file.Read(extra[:]); n != 0 || (err != nil && err != io.EOF) {
			return errors.New("file changed or exceeded byte limit")
		}
		return nil
	}
	if strings.HasSuffix(path, "/") || filepath.Clean(path) == "/" {
		return errors.New("file write requires a file destination")
	}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return err
	}
	file, err := os.CreateTemp(parent, ".ucloud-write-*")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	defer file.Close()
	count, err := io.Copy(file, io.LimitReader(input, limit+1))
	if err != nil {
		return err
	}
	if count > limit {
		return errors.New("file exceeds byte limit")
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	// Rename replaces a destination symlink rather than following it. Existing
	// directories fail; the old destination survives every pre-rename failure.
	if err := os.Rename(temporary, path); err != nil {
		return fmt.Errorf("replace file: %w", err)
	}
	return nil
}
