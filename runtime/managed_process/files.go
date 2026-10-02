package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

const maxFileOperationBytes = 256 * 1024 * 1024

var (
	errFileInvalid    = errors.New("invalid file request")
	errFileTooLarge   = errors.New("file exceeds byte limit")
	errFileNotRegular = errors.New("not a regular file")
)

// Exit statuses of `files`, which the guest agent maps to protocol error codes
// (agent_linux.go). Every failure is non-zero, as callers checking only that
// still expect. 2 is skipped: a Go panic or fatal runtime error exits with it,
// and must read as "failed", not as the caller's mistake.
const (
	fileExitFailed     = 1
	fileExitInvalid    = 3
	fileExitNotFound   = 4
	fileExitPermission = 5
	fileExitTooLarge   = 6
	fileExitNotRegular = 7
)

func fileExitCode(err error) int {
	switch {
	case errors.Is(err, errFileInvalid):
		return fileExitInvalid
	case errors.Is(err, fs.ErrNotExist):
		return fileExitNotFound
	case errors.Is(err, fs.ErrPermission):
		return fileExitPermission
	case errors.Is(err, errFileTooLarge):
		return fileExitTooLarge
	case errors.Is(err, errFileNotRegular), errors.Is(err, syscall.EISDIR):
		return fileExitNotRegular
	}
	return fileExitFailed
}

// fileStat is the `files stat` output and the guest agent's stat result.
type fileStat struct {
	Type    string `json:"type"`
	Size    int64  `json:"size"`
	Mode    uint32 `json:"mode"`
	MtimeNs int64  `json:"mtime_ns"`
	UID     uint32 `json:"uid"`
	GID     uint32 `json:"gid"`
}

func validateFilePath(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("%w: file path must be absolute", errFileInvalid)
	}
	for _, part := range strings.Split(path, "/") {
		if part == ".." {
			return fmt.Errorf("%w: parent traversal is unsupported", errFileInvalid)
		}
	}
	for _, char := range path {
		if char < 32 || char == 127 {
			return fmt.Errorf("%w: file path contains control characters", errFileInvalid)
		}
	}
	return nil
}

// These operations run inside the guest under the exec identity. They are not
// a host filesystem broker or an authorization boundary against guest root.
func runFiles(args []string, input io.Reader, output io.Writer) error {
	if len(args) == 1 && args[0] == "ready" {
		return nil
	}
	if len(args) == 2 && args[0] == "stat" {
		if err := validateFilePath(args[1]); err != nil {
			return err
		}
		return statFile(args[1], output)
	}
	if len(args) != 3 {
		return fmt.Errorf("%w: usage: files read|write absolute-path max-bytes | stat absolute-path", errFileInvalid)
	}
	operation, path := args[0], args[1]
	if operation != "read" && operation != "write" {
		return fmt.Errorf("%w: unsupported file operation", errFileInvalid)
	}
	if err := validateFilePath(path); err != nil {
		return err
	}
	limit, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil || limit < 1 || limit > maxFileOperationBytes {
		return fmt.Errorf("%w: file limit must be 1..268435456 bytes", errFileInvalid)
	}
	if operation == "read" {
		return readFile(path, limit, output)
	}
	return writeFile(path, limit, input)
}

func readFile(path string, limit int64, output io.Writer) error {
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
		return fmt.Errorf("%w: file read requires a regular file", errFileNotRegular)
	}
	if info.Size() > limit {
		return errFileTooLarge
	}
	if _, err = io.Copy(output, io.LimitReader(file, limit)); err != nil {
		return err
	}
	var extra [1]byte
	if n, err := file.Read(extra[:]); n != 0 || (err != nil && err != io.EOF) {
		return fmt.Errorf("%w: file changed while it was read", errFileTooLarge)
	}
	return nil
}

func writeFile(path string, limit int64, input io.Reader) error {
	if strings.HasSuffix(path, "/") || filepath.Clean(path) == "/" {
		return fmt.Errorf("%w: file write requires a file destination", errFileInvalid)
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
		return errFileTooLarge
	}
	// File upload promises atomic visibility, like the shell implementation.
	// The lifecycle capture barrier owns durable workspace synchronization.
	// A per-upload fsync adds storage latency to every generated tool without
	// making the subsequent rename crash-durable (that needs a directory sync).
	if err := file.Close(); err != nil {
		return err
	}
	// Rename replaces a destination symlink rather than following it. Existing
	// directories fail; the old destination survives every pre-rename failure.
	if err := os.Rename(temporary, path); err != nil {
		// os.Rename reports a directory destination as EEXIST.
		if errors.Is(err, syscall.EEXIST) || errors.Is(err, syscall.EISDIR) {
			return fmt.Errorf("%w: file write destination is a directory", errFileNotRegular)
		}
		return fmt.Errorf("replace file: %w", err)
	}
	return nil
}

// statFile follows symlinks, as reading does.
func statFile(path string, output io.Writer) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	result := fileStat{Type: "other", Size: info.Size(), Mode: uint32(info.Mode().Perm()), MtimeNs: info.ModTime().UnixNano()}
	switch {
	case info.Mode().IsRegular():
		result.Type = "file"
	case info.IsDir():
		result.Type = "directory"
	}
	if system, ok := info.Sys().(*syscall.Stat_t); ok {
		result.Mode = uint32(system.Mode) & 0o7777
		result.UID, result.GID = system.Uid, system.Gid
	}
	return json.NewEncoder(output).Encode(result)
}
