package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	protocolVersion = 1
	defaultStateDir = "/.ucloud-managed"
	buildVersion    = "managed-primary-v1"
	maxRequestBytes = 1024 * 1024
	maxLogReadBytes = 1024 * 1024
)

var (
	jobIDPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$`)
	envKeyPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

type request struct {
	Version        int               `json:"version"`
	Action         string            `json:"action"`
	JobID          string            `json:"job_id,omitempty"`
	OperationID    string            `json:"operation_id,omitempty"`
	Argv           []string          `json:"argv,omitempty"`
	Env            map[string]string `json:"env,omitempty"`
	Cwd            string            `json:"cwd,omitempty"`
	UID            uint32            `json:"uid,omitempty"`
	GID            uint32            `json:"gid,omitempty"`
	MaxStdoutBytes int64             `json:"max_stdout_bytes,omitempty"`
	MaxStderrBytes int64             `json:"max_stderr_bytes,omitempty"`
	Stream         string            `json:"stream,omitempty"`
	Offset         int64             `json:"offset,omitempty"`
	Limit          int               `json:"limit,omitempty"`
	Signal         int               `json:"signal,omitempty"`
}

type jobSpec struct {
	JobID          string            `json:"job_id"`
	Argv           []string          `json:"argv"`
	Env            map[string]string `json:"env"`
	Cwd            string            `json:"cwd"`
	UID            uint32            `json:"uid"`
	GID            uint32            `json:"gid"`
	MaxStdoutBytes int64             `json:"max_stdout_bytes"`
	MaxStderrBytes int64             `json:"max_stderr_bytes"`
}

type jobRecord struct {
	Version         int    `json:"version"`
	JobID           string `json:"job_id"`
	SpecSHA256      string `json:"spec_sha256"`
	State           string `json:"state"`
	PID             int    `json:"pid,omitempty"`
	StartedAt       string `json:"started_at,omitempty"`
	CompletedAt     string `json:"completed_at,omitempty"`
	ExitCode        *int   `json:"exit_code,omitempty"`
	Signal          int    `json:"signal,omitempty"`
	StdoutBytes     int64  `json:"stdout_bytes"`
	StderrBytes     int64  `json:"stderr_bytes"`
	StdoutTruncated bool   `json:"stdout_truncated"`
	StderrTruncated bool   `json:"stderr_truncated"`
	Sequence        uint64 `json:"sequence"`
}

type response struct {
	Version int        `json:"version"`
	OK      bool       `json:"ok"`
	Error   string     `json:"error,omitempty"`
	Job     *jobRecord `json:"job,omitempty"`
	Stream  string     `json:"stream,omitempty"`
	Offset  int64      `json:"offset,omitempty"`
	Next    int64      `json:"next_offset,omitempty"`
	EOF     bool       `json:"eof,omitempty"`
	Data    []byte     `json:"data,omitempty"`
}

type boundedWriter struct {
	mu        sync.Mutex
	file      *os.File
	max       int64
	retained  int64
	truncated bool
}

func newBoundedWriter(path string, max int64) (*boundedWriter, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	return &boundedWriter{file: file, max: max, retained: info.Size()}, nil
}

func (writer *boundedWriter) Write(payload []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	original := len(payload)
	remaining := writer.max - writer.retained
	if remaining <= 0 {
		writer.truncated = writer.truncated || original > 0
		return original, nil
	}
	if int64(len(payload)) > remaining {
		payload = payload[:remaining]
		writer.truncated = true
	}
	if len(payload) > 0 {
		written, err := writer.file.Write(payload)
		writer.retained += int64(written)
		if err != nil {
			return written, err
		}
		if written != len(payload) {
			return written, io.ErrShortWrite
		}
	}
	return original, nil
}

func (writer *boundedWriter) snapshot() (int64, bool) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.retained, writer.truncated
}

func (writer *boundedWriter) close() {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	_ = writer.file.Sync()
	_ = writer.file.Close()
}

type supervisor struct {
	mu       sync.Mutex
	stateDir string
	socket   string
	state    *jobRecord
	command  *exec.Cmd
	stdout   *boundedWriter
	stderr   *boundedWriter
}

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: %s supervise|ctl", os.Args[0])
	}
	switch os.Args[1] {
	case "supervise":
		if err := runSupervisor(os.Args[2:]); err != nil {
			fatalf("supervisor failed: %v", err)
		}
	case "ctl":
		if err := runControl(os.Args[2:]); err != nil {
			fatalf("job control failed: %v", err)
		}
	case "launch":
		if err := runLaunch(); err != nil {
			fatalf("job launch failed: %v", err)
		}
	case "version", "--version":
		fmt.Println(buildVersion)
	default:
		fatalf("unsupported mode: %s", os.Args[1])
	}
}

func runSupervisor(arguments []string) error {
	flags := flag.NewFlagSet("supervise", flag.ContinueOnError)
	stateDir := flags.String("state-dir", defaultStateDir, "private managed-process state")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if !filepath.IsAbs(*stateDir) {
		return errors.New("state directory must be absolute")
	}
	if err := os.MkdirAll(*stateDir, 0700); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}
	if err := os.Chmod(*stateDir, 0700); err != nil {
		return fmt.Errorf("secure state directory: %w", err)
	}
	socket := filepath.Join(*stateDir, "control.sock")
	_ = os.Remove(socket)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return fmt.Errorf("listen on control socket: %w", err)
	}
	defer listener.Close()
	defer os.Remove(socket)
	if err := os.Chmod(socket, 0600); err != nil {
		return fmt.Errorf("secure control socket: %w", err)
	}
	s := &supervisor{stateDir: *stateDir, socket: socket}
	if err := s.loadTerminalState(); err != nil {
		return err
	}

	signals := make(chan os.Signal, 8)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT)
	go func() {
		for received := range signals {
			s.forwardSignal(received.(syscall.Signal))
		}
	}()

	for {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			if errors.Is(acceptErr, net.ErrClosed) {
				return nil
			}
			continue
		}
		go s.serve(connection)
	}
}

func (s *supervisor) loadTerminalState() error {
	path := filepath.Join(s.stateDir, "state.json")
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read state: %w", err)
	}
	var record jobRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return fmt.Errorf("decode state: %w", err)
	}
	if record.Version != protocolVersion || record.JobID == "" {
		return errors.New("persisted state is invalid")
	}
	// A live supervisor is restored in memory by gVisor. Reading a running row
	// here means the supervisor itself restarted, so the child cannot still be
	// owned safely.
	if record.State == "running" || record.State == "starting" {
		record.State = "failed"
		record.PID = 0
		record.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
		record.Sequence++
	}
	s.state = &record
	return s.persistLocked()
}

func (s *supervisor) serve(connection net.Conn) {
	defer connection.Close()
	decoder := json.NewDecoder(io.LimitReader(connection, maxRequestBytes+1))
	decoder.DisallowUnknownFields()
	var req request
	if err := decoder.Decode(&req); err != nil {
		writeResponse(connection, response{Version: protocolVersion, Error: "invalid request"})
		return
	}
	if req.Version != protocolVersion {
		writeResponse(connection, response{Version: protocolVersion, Error: "unsupported protocol version"})
		return
	}
	var reply response
	switch req.Action {
	case "start":
		reply = s.start(req)
	case "status":
		reply = s.status(req.JobID)
	case "logs":
		reply = s.logs(req)
	case "signal":
		reply = s.signal(req)
	default:
		reply = response{Version: protocolVersion, Error: "unsupported action"}
	}
	writeResponse(connection, reply)
}

func (s *supervisor) start(req request) response {
	spec, digest, err := validatedSpec(req)
	if err != nil {
		return response{Version: protocolVersion, Error: err.Error()}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != nil {
		if s.state.JobID == spec.JobID && s.state.SpecSHA256 == digest {
			snapshot := s.snapshotLocked()
			return response{Version: protocolVersion, OK: true, Job: &snapshot}
		}
		return response{Version: protocolVersion, Error: "sandbox generation already owns another primary process"}
	}
	stdout, err := newBoundedWriter(filepath.Join(s.stateDir, "stdout.log"), spec.MaxStdoutBytes)
	if err != nil {
		return response{Version: protocolVersion, Error: "open stdout spool: " + err.Error()}
	}
	stderr, err := newBoundedWriter(filepath.Join(s.stateDir, "stderr.log"), spec.MaxStderrBytes)
	if err != nil {
		stdout.close()
		return response{Version: protocolVersion, Error: "open stderr spool: " + err.Error()}
	}
	launchReader, launchWriter, err := os.Pipe()
	if err != nil {
		stdout.close()
		stderr.close()
		return response{Version: protocolVersion, Error: "open launch pipe: " + err.Error()}
	}
	cmd := exec.Command(os.Args[0], "launch")
	cmd.Env = mergeEnvironment(
		os.Environ(),
		map[string]string{"UCLOUD_JOB_LAUNCH_FD": "3"},
	)
	cmd.ExtraFiles = []*os.File{launchReader}
	cmd.Stdin = nil
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if os.Geteuid() != 0 && (int(spec.UID) != os.Geteuid() || int(spec.GID) != os.Getegid()) {
		launchReader.Close()
		launchWriter.Close()
		stdout.close()
		stderr.close()
		return response{Version: protocolVersion, Error: "supervisor cannot change workload credentials"}
	}
	record := &jobRecord{
		Version:    protocolVersion,
		JobID:      spec.JobID,
		SpecSHA256: digest,
		State:      "starting",
		StartedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		Sequence:   1,
	}
	s.state = record
	s.stdout = stdout
	s.stderr = stderr
	if err := s.persistLocked(); err != nil {
		s.state = nil
		stdout.close()
		stderr.close()
		return response{Version: protocolVersion, Error: "persist admission: " + err.Error()}
	}
	if err := cmd.Start(); err != nil {
		launchReader.Close()
		launchWriter.Close()
		record.State = "failed"
		record.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
		record.Sequence++
		stdout.close()
		stderr.close()
		_ = s.persistLocked()
		snapshot := s.snapshotLocked()
		return response{Version: protocolVersion, OK: true, Job: &snapshot}
	}
	launchReader.Close()
	if err := json.NewEncoder(launchWriter).Encode(spec); err != nil {
		launchWriter.Close()
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		record.State = "failed"
		record.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
		record.Sequence++
		stdout.close()
		stderr.close()
		_ = s.persistLocked()
		snapshot := s.snapshotLocked()
		return response{Version: protocolVersion, OK: true, Job: &snapshot}
	}
	launchWriter.Close()
	s.command = cmd
	record.State = "running"
	record.PID = cmd.Process.Pid
	record.Sequence++
	_ = s.persistLocked()
	go s.wait(cmd)
	snapshot := s.snapshotLocked()
	return response{Version: protocolVersion, OK: true, Job: &snapshot}
}

func (s *supervisor) wait(cmd *exec.Cmd) {
	err := cmd.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.command != cmd || s.state == nil {
		return
	}
	stdoutBytes, stdoutTruncated := s.stdout.snapshot()
	stderrBytes, stderrTruncated := s.stderr.snapshot()
	s.stdout.close()
	s.stderr.close()
	s.state.StdoutBytes = stdoutBytes
	s.state.StderrBytes = stderrBytes
	s.state.StdoutTruncated = stdoutTruncated
	s.state.StderrTruncated = stderrTruncated
	s.state.PID = 0
	s.state.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
	s.state.Sequence++
	s.state.State = "exited"
	if exitError, ok := err.(*exec.ExitError); ok {
		if status, ok := exitError.Sys().(syscall.WaitStatus); ok {
			if status.Signaled() {
				s.state.State = "signaled"
				s.state.Signal = int(status.Signal())
			}
			code := status.ExitStatus()
			s.state.ExitCode = &code
		}
	} else if err != nil {
		s.state.State = "failed"
	} else {
		code := 0
		s.state.ExitCode = &code
	}
	s.command = nil
	_ = s.persistLocked()
}

func (s *supervisor) status(jobID string) response {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == nil || (jobID != "" && jobID != s.state.JobID) {
		return response{Version: protocolVersion, Error: "primary process not found"}
	}
	snapshot := s.snapshotLocked()
	return response{Version: protocolVersion, OK: true, Job: &snapshot}
}

func (s *supervisor) logs(req request) response {
	if req.Stream != "stdout" && req.Stream != "stderr" {
		return response{Version: protocolVersion, Error: "stream must be stdout or stderr"}
	}
	if req.Offset < 0 {
		return response{Version: protocolVersion, Error: "log offset cannot be negative"}
	}
	limit := req.Limit
	if limit <= 0 || limit > maxLogReadBytes {
		limit = maxLogReadBytes
	}
	s.mu.Lock()
	if s.state == nil || (req.JobID != "" && req.JobID != s.state.JobID) {
		s.mu.Unlock()
		return response{Version: protocolVersion, Error: "primary process not found"}
	}
	path := filepath.Join(s.stateDir, req.Stream+".log")
	s.mu.Unlock()
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return response{Version: protocolVersion, OK: true, Stream: req.Stream, Offset: req.Offset, Next: req.Offset, EOF: true}
		}
		return response{Version: protocolVersion, Error: "read log: " + err.Error()}
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return response{Version: protocolVersion, Error: "stat log: " + err.Error()}
	}
	if req.Offset > info.Size() {
		return response{Version: protocolVersion, Error: "log offset exceeds retained data"}
	}
	if _, err := file.Seek(req.Offset, io.SeekStart); err != nil {
		return response{Version: protocolVersion, Error: "seek log: " + err.Error()}
	}
	payload, err := io.ReadAll(io.LimitReader(file, int64(limit)))
	if err != nil {
		return response{Version: protocolVersion, Error: "read log: " + err.Error()}
	}
	next := req.Offset + int64(len(payload))
	return response{Version: protocolVersion, OK: true, Stream: req.Stream, Offset: req.Offset, Next: next, EOF: next >= info.Size(), Data: payload}
}

func (s *supervisor) signal(req request) response {
	if req.Signal <= 0 || req.Signal > 64 || req.Signal == int(syscall.SIGKILL) || req.Signal == int(syscall.SIGSTOP) {
		return response{Version: protocolVersion, Error: "signal is not supported"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == nil || req.JobID != s.state.JobID {
		return response{Version: protocolVersion, Error: "primary process not found"}
	}
	if s.state.State != "running" || s.command == nil || s.command.Process == nil {
		snapshot := s.snapshotLocked()
		return response{Version: protocolVersion, OK: true, Job: &snapshot}
	}
	if err := syscall.Kill(-s.command.Process.Pid, syscall.Signal(req.Signal)); err != nil && !errors.Is(err, syscall.ESRCH) {
		return response{Version: protocolVersion, Error: "signal primary process: " + err.Error()}
	}
	snapshot := s.snapshotLocked()
	return response{Version: protocolVersion, OK: true, Job: &snapshot}
}

func (s *supervisor) forwardSignal(received syscall.Signal) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.command == nil || s.command.Process == nil {
		return
	}
	_ = syscall.Kill(-s.command.Process.Pid, received)
}

func (s *supervisor) snapshotLocked() jobRecord {
	record := *s.state
	if s.stdout != nil {
		record.StdoutBytes, record.StdoutTruncated = s.stdout.snapshot()
	}
	if s.stderr != nil {
		record.StderrBytes, record.StderrTruncated = s.stderr.snapshot()
	}
	return record
}

func (s *supervisor) persistLocked() error {
	if s.state == nil {
		return nil
	}
	payload, err := json.Marshal(s.state)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	temporary, err := os.CreateTemp(s.stateDir, ".state.*.tmp")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryName, filepath.Join(s.stateDir, "state.json")); err != nil {
		return err
	}
	directory, err := os.Open(s.stateDir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func runControl(arguments []string) error {
	flags := flag.NewFlagSet("ctl", flag.ContinueOnError)
	socket := flags.String("socket", filepath.Join(defaultStateDir, "control.sock"), "managed-process socket")
	timeout := flags.Duration("timeout", 10*time.Second, "control timeout")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	payload, err := io.ReadAll(io.LimitReader(os.Stdin, maxRequestBytes+1))
	if err != nil {
		return err
	}
	if len(payload) > maxRequestBytes {
		return errors.New("control request exceeds limit")
	}
	connection, err := net.DialTimeout("unix", *socket, *timeout)
	if err != nil {
		return err
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(*timeout))
	if _, err := connection.Write(payload); err != nil {
		return err
	}
	if unix, ok := connection.(*net.UnixConn); ok {
		_ = unix.CloseWrite()
	}
	reply, err := io.ReadAll(io.LimitReader(connection, maxRequestBytes+1))
	if err != nil {
		return err
	}
	if len(reply) > maxRequestBytes {
		return errors.New("control response exceeds limit")
	}
	var decoded response
	if err := json.Unmarshal(reply, &decoded); err != nil {
		return fmt.Errorf("invalid control response: %w", err)
	}
	if _, err := os.Stdout.Write(reply); err != nil {
		return err
	}
	if !decoded.OK {
		return errors.New(decoded.Error)
	}
	return nil
}

func runLaunch() error {
	if os.Getenv("UCLOUD_JOB_LAUNCH_FD") != "3" {
		return errors.New("launch descriptor is invalid")
	}
	launchPipe := os.NewFile(3, "managed-process-launch")
	if launchPipe == nil {
		return errors.New("launch descriptor is unavailable")
	}
	var spec jobSpec
	decoder := json.NewDecoder(io.LimitReader(launchPipe, maxRequestBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&spec); err != nil {
		launchPipe.Close()
		return fmt.Errorf("decode launch spec: %w", err)
	}
	launchPipe.Close()
	if len(spec.Argv) == 0 || !filepath.IsAbs(spec.Cwd) {
		return errors.New("launch spec is invalid")
	}
	if err := dropWorkloadPrivileges(spec.UID, spec.GID); err != nil {
		return fmt.Errorf("drop workload privileges: %w", err)
	}
	if err := os.Chdir(spec.Cwd); err != nil {
		return fmt.Errorf("change workload directory: %w", err)
	}
	_ = os.Unsetenv("UCLOUD_JOB_LAUNCH_FD")
	environment := mergeEnvironment(os.Environ(), spec.Env)
	executable, err := resolveExecutable(spec.Argv[0], environment)
	if err != nil {
		return fmt.Errorf("resolve workload executable: %w", err)
	}
	if err := syscall.Exec(executable, spec.Argv, environment); err != nil {
		return fmt.Errorf("exec workload: %w", err)
	}
	return nil
}

func resolveExecutable(name string, environment []string) (string, error) {
	if strings.ContainsRune(name, filepath.Separator) {
		return name, nil
	}
	pathValue := ""
	for _, item := range environment {
		key, value, found := strings.Cut(item, "=")
		if found && key == "PATH" {
			pathValue = value
			break
		}
	}
	for _, directory := range filepath.SplitList(pathValue) {
		if directory == "" {
			directory = "."
		}
		candidate := filepath.Join(directory, name)
		info, err := os.Stat(candidate)
		if err != nil || info.IsDir() || info.Mode()&0111 == 0 {
			continue
		}
		return candidate, nil
	}
	return "", exec.ErrNotFound
}

func validatedSpec(req request) (jobSpec, string, error) {
	if !jobIDPattern.MatchString(req.JobID) {
		return jobSpec{}, "", errors.New("job_id is invalid")
	}
	if len(req.Argv) == 0 || len(req.Argv) > 4096 {
		return jobSpec{}, "", errors.New("argv must be non-empty and bounded")
	}
	for _, item := range req.Argv {
		if strings.IndexByte(item, 0) >= 0 {
			return jobSpec{}, "", errors.New("argv contains NUL")
		}
	}
	if !filepath.IsAbs(req.Cwd) {
		return jobSpec{}, "", errors.New("cwd must be absolute")
	}
	for key, value := range req.Env {
		if !envKeyPattern.MatchString(key) || strings.IndexByte(value, 0) >= 0 {
			return jobSpec{}, "", errors.New("environment is invalid")
		}
	}
	if req.MaxStdoutBytes <= 0 || req.MaxStderrBytes <= 0 {
		return jobSpec{}, "", errors.New("log limits must be positive")
	}
	spec := jobSpec{
		JobID:          req.JobID,
		Argv:           append([]string(nil), req.Argv...),
		Env:            cloneMap(req.Env),
		Cwd:            req.Cwd,
		UID:            req.UID,
		GID:            req.GID,
		MaxStdoutBytes: req.MaxStdoutBytes,
		MaxStderrBytes: req.MaxStderrBytes,
	}
	payload, err := json.Marshal(spec)
	if err != nil {
		return jobSpec{}, "", err
	}
	digest := sha256.Sum256(payload)
	return spec, hex.EncodeToString(digest[:]), nil
}

func mergeEnvironment(base []string, overrides map[string]string) []string {
	values := make(map[string]string, len(base)+len(overrides))
	for _, item := range base {
		key, value, found := strings.Cut(item, "=")
		if found {
			values[key] = value
		}
	}
	for key, value := range overrides {
		values[key] = value
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+values[key])
	}
	return result
}

func cloneMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func writeResponse(writer io.Writer, reply response) {
	_ = json.NewEncoder(writer).Encode(reply)
}

func fatalf(format string, values ...any) {
	writer := bufio.NewWriter(os.Stderr)
	fmt.Fprintf(writer, format+"\n", values...)
	writer.Flush()
	os.Exit(1)
}
