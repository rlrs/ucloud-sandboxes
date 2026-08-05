package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") == "1" {
		if len(os.Args) < 2 {
			os.Exit(2)
		}
		var err error
		switch os.Args[1] {
		case "supervise":
			err = runSupervisor(os.Args[2:])
		case "launch":
			err = runLaunch()
		case "ctl":
			err = runControl(os.Args[2:])
		default:
			os.Exit(2)
		}
		if err != nil {
			os.Exit(3)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestControlAcceptsMaximumLogResponse(t *testing.T) {
	stateDir := t.TempDir()
	socket := filepath.Join(stateDir, "control.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	serverErrors := make(chan error, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverErrors <- acceptErr
			return
		}
		defer connection.Close()
		if _, readErr := io.ReadAll(connection); readErr != nil {
			serverErrors <- readErr
			return
		}
		payload, marshalErr := json.Marshal(response{
			Version: protocolVersion,
			OK:      true,
			Stream:  "stdout",
			Next:    maxLogReadBytes,
			EOF:     true,
			Data:    bytes.Repeat([]byte{0xa5}, maxLogReadBytes),
		})
		if marshalErr != nil {
			serverErrors <- marshalErr
			return
		}
		if len(payload) <= maxRequestBytes || len(payload) > maxResponseBytes {
			serverErrors <- fmt.Errorf("unexpected encoded response size: %d", len(payload))
			return
		}
		_, writeErr := io.Copy(connection, bytes.NewReader(payload))
		serverErrors <- writeErr
	}()

	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "ctl", "--socket", socket)
	command.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	command.Stdin = bytes.NewBufferString(`{"version":1,"action":"logs"}`)
	output, err := command.Output()
	if err != nil {
		t.Fatalf("maximum log response was rejected: %v", err)
	}
	if serverErr := <-serverErrors; serverErr != nil {
		t.Fatal(serverErr)
	}
	var decoded response
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Data) != maxLogReadBytes {
		t.Fatalf("expected %d log bytes, got %d", maxLogReadBytes, len(decoded.Data))
	}
}

func TestSupervisorOwnsIdempotentBoundedPrimaryProcess(t *testing.T) {
	stateDir, err := os.MkdirTemp("/tmp", "ucmp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(stateDir) })
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	server := exec.Command(binary, "supervise", "--state-dir", stateDir)
	server.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = server.Process.Kill()
		_, _ = server.Process.Wait()
	})
	socket := filepath.Join(stateDir, "control.sock")
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(socket); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("supervisor socket did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}

	start := request{
		Version:        protocolVersion,
		Action:         "start",
		JobID:          "job-1",
		Argv:           []string{"sh", "-c", "printf 123456789; printf err >&2"},
		Env:            map[string]string{"PATH": "/bin:/usr/bin", "TEST_VALUE": "ok"},
		Cwd:            "/",
		UID:            uint32(os.Getuid()),
		GID:            uint32(os.Getgid()),
		MaxStdoutBytes: 5,
		MaxStderrBytes: 8,
	}
	first := callSupervisor(t, socket, start)
	if !first.OK || first.Job == nil || first.Job.JobID != "job-1" {
		t.Fatalf("unexpected start response: %#v", first)
	}
	second := callSupervisor(t, socket, start)
	if !second.OK || second.Job == nil || second.Job.SpecSHA256 != first.Job.SpecSHA256 {
		t.Fatalf("idempotent start changed identity: %#v", second)
	}

	var status response
	deadline = time.Now().Add(5 * time.Second)
	for {
		status = callSupervisor(t, socket, request{Version: protocolVersion, Action: "status", JobID: "job-1"})
		if status.Job != nil && status.Job.State != "running" && status.Job.State != "starting" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("primary process did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status.Job.State != "exited" || status.Job.ExitCode == nil || *status.Job.ExitCode != 0 {
		t.Fatalf("unexpected terminal state: %#v", status.Job)
	}
	if status.Job.StdoutBytes != 5 || !status.Job.StdoutTruncated {
		t.Fatalf("stdout was not bounded: %#v", status.Job)
	}

	logs := callSupervisor(t, socket, request{
		Version: protocolVersion,
		Action:  "logs",
		JobID:   "job-1",
		Stream:  "stdout",
		Limit:   32,
	})
	if !logs.OK || string(logs.Data) != "12345" || logs.Next != 5 || !logs.EOF {
		t.Fatalf("unexpected logs: %#v", logs)
	}

	conflict := start
	conflict.JobID = "job-2"
	conflicted := callSupervisor(t, socket, conflict)
	if conflicted.OK || conflicted.Error == "" {
		t.Fatalf("second primary process was admitted: %#v", conflicted)
	}
}

func TestStatusSurfacesAndRetriesDurabilityFailures(t *testing.T) {
	for _, state := range []string{"running", "exited"} {
		t.Run(state, func(t *testing.T) {
			stateDir := t.TempDir()
			s := &supervisor{
				stateDir: stateDir,
				state: &jobRecord{
					Version:    protocolVersion,
					JobID:      "job-1",
					SpecSHA256: strings.Repeat("a", 64),
					State:      state,
					Sequence:   2,
				},
			}
			injected := errors.New("injected persistence failure")
			failing := true
			s.persistHook = func(_ *jobRecord) error {
				if failing {
					return injected
				}
				return nil
			}

			s.mu.Lock()
			if err := s.persistStateLocked(); !errors.Is(err, injected) {
				t.Fatalf("expected injected persistence failure, got %v", err)
			}
			s.mu.Unlock()

			degraded := s.status("job-1")
			if degraded.OK || degraded.Job == nil || degraded.Job.State != state {
				t.Fatalf("durability failure was not visible: %#v", degraded)
			}
			if !strings.Contains(degraded.Error, "durability is degraded") {
				t.Fatalf("unexpected durability error: %q", degraded.Error)
			}
			degradedLogs := s.logs(request{
				JobID:  "job-1",
				Stream: "stdout",
				Limit:  1,
			})
			if degradedLogs.OK || degradedLogs.Job == nil {
				t.Fatalf("log request hid durability failure: %#v", degradedLogs)
			}

			failing = false
			recovered := s.status("job-1")
			if !recovered.OK || recovered.Error != "" || recovered.Job == nil {
				t.Fatalf("durability retry did not recover: %#v", recovered)
			}
			payload, err := os.ReadFile(filepath.Join(stateDir, "state.json"))
			if err != nil {
				t.Fatal(err)
			}
			var persisted jobRecord
			if err := json.Unmarshal(payload, &persisted); err != nil {
				t.Fatal(err)
			}
			if persisted.State != state || persisted.Sequence != 2 {
				t.Fatalf("unexpected persisted state: %#v", persisted)
			}
		})
	}
}

func callSupervisor(t *testing.T, socket string, req request) response {
	t.Helper()
	connection, err := net.DialTimeout("unix", socket, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := json.NewEncoder(connection).Encode(req); err != nil {
		t.Fatal(err)
	}
	var reply response
	if err := json.NewDecoder(connection).Decode(&reply); err != nil {
		t.Fatal(err)
	}
	return reply
}
