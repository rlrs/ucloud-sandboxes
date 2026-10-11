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
		case "files":
			if err := runFiles(os.Args[2:], os.Stdin, os.Stdout); err != nil {
				fmt.Fprintf(os.Stderr, "file operation failed: %v\n", err)
				os.Exit(fileExitCode(err))
			}
			os.Exit(0)
		case "agent":
			err = runAgent(os.Args[2:])
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

func TestDecodeControlRequestIsBoundedAndConsumesOneObject(t *testing.T) {
	valid := []byte(`{"version":1,"action":"status","job_id":"job-1"}`)
	decoded, err := decodeControlRequest(bytes.NewReader(valid))
	if err != nil || decoded.Action != "status" || decoded.JobID != "job-1" {
		t.Fatalf("valid request did not decode: %#v, %v", decoded, err)
	}
	for name, payload := range map[string][]byte{
		"unknown field":    []byte(`{"version":1,"action":"status","extra":true}`),
		"trailing object":  append(append([]byte{}, valid...), []byte(` {}`)...),
		"trailing garbage": append(append([]byte{}, valid...), []byte(` nope`)...),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeControlRequest(bytes.NewReader(payload)); err == nil {
				t.Fatal("malformed request was accepted")
			}
		})
	}
	oversized := []byte(`{"version":1,"action":"status","job_id":"` +
		strings.Repeat("a", maxRequestBytes) + `"}`)
	if _, err := decodeControlRequest(bytes.NewReader(oversized)); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("oversized request did not fail at the byte limit: %v", err)
	}
}

func TestSupervisorRespondsWithoutClientHalfClose(t *testing.T) {
	reply := serveRawControlRequest(t, []byte(`{"version":1,"action":"status","job_id":"job-1"}`))
	if reply.Error != "primary process not found" {
		t.Fatalf("valid request was rejected: %#v", reply)
	}
}

func TestSupervisorRejectsTrailingAndOversizedRequests(t *testing.T) {
	valid := []byte(`{"version":1,"action":"status","job_id":"job-1"}`)
	for name, payload := range map[string][]byte{
		"trailing object":  append(append([]byte{}, valid...), []byte(` {}`)...),
		"trailing garbage": append(append([]byte{}, valid...), []byte(` nope`)...),
		"oversized": []byte(`{"version":1,"action":"status","job_id":"` +
			strings.Repeat("a", maxRequestBytes) + `"}`),
	} {
		t.Run(name, func(t *testing.T) {
			reply := serveRawControlRequest(t, payload)
			if reply.Error != "invalid request" {
				t.Fatalf("malformed request was accepted: %#v", reply)
			}
		})
	}
}

func serveRawControlRequest(t *testing.T, requestPayload []byte) response {
	t.Helper()
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() {
		(&supervisor{}).serve(server)
		close(done)
	}()
	defer client.Close()
	if err := client.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	writeDone := make(chan struct{})
	go func() {
		_, _ = client.Write(requestPayload)
		close(writeDone)
	}()
	var reply response
	if err := json.NewDecoder(client).Decode(&reply); err != nil {
		t.Fatalf("supervisor waited for client EOF instead of replying: %v", err)
	}
	_ = client.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("supervisor did not finish the one-request connection")
	}
	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("request writer did not finish")
	}
	return reply
}

func TestControlAcceptsMaximumLogResponse(t *testing.T) {
	stateDir, err := os.MkdirTemp("/tmp", "ucmp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(stateDir) })
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

func TestLoadTerminalStateFailsProcessesLostWithSupervisor(t *testing.T) {
	for _, state := range []string{"starting", "running"} {
		t.Run(state, func(t *testing.T) {
			stateDir := t.TempDir()
			original := jobRecord{
				Version:    protocolVersion,
				JobID:      "job-1",
				SpecSHA256: strings.Repeat("a", 64),
				State:      state,
				PID:        1234,
				Sequence:   7,
			}
			payload, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(stateDir, "state.json"), payload, 0600); err != nil {
				t.Fatal(err)
			}

			s := &supervisor{stateDir: stateDir}
			if err := s.loadTerminalState(); err != nil {
				t.Fatal(err)
			}
			if s.state == nil || s.state.State != "failed" || s.state.PID != 0 || s.state.Sequence != 8 {
				t.Fatalf("lost process was not failed on restart: %#v", s.state)
			}
			if _, err := time.Parse(time.RFC3339Nano, s.state.CompletedAt); err != nil {
				t.Fatalf("restart completion time is invalid: %q", s.state.CompletedAt)
			}

			persistedPayload, err := os.ReadFile(filepath.Join(stateDir, "state.json"))
			if err != nil {
				t.Fatal(err)
			}
			var persisted jobRecord
			if err := json.Unmarshal(persistedPayload, &persisted); err != nil {
				t.Fatal(err)
			}
			if persisted.State != "failed" || persisted.PID != 0 || persisted.Sequence != 8 {
				t.Fatalf("restart conversion was not persisted: %#v", persisted)
			}
		})
	}
}

func readPersistedState(t *testing.T, stateDir string) jobRecord {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join(stateDir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var persisted jobRecord
	if err := json.Unmarshal(payload, &persisted); err != nil {
		t.Fatal(err)
	}
	return persisted
}

func waitForPersistedState(t *testing.T, stateDir string, ready func(jobRecord) bool) jobRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		persisted := readPersistedState(t, stateDir)
		if ready(persisted) {
			return persisted
		}
		if time.Now().After(deadline) {
			t.Fatalf("persisted state did not reach the expected shape: %#v", persisted)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFirstTruncationIsPersistedOnceWithoutBlockingTheWriter(t *testing.T) {
	stateDir := t.TempDir()
	stdout, err := newBoundedWriter(filepath.Join(stateDir, "stdout.log"), 4)
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := newBoundedWriter(filepath.Join(stateDir, "stderr.log"), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stdout.close)
	t.Cleanup(stderr.close)
	s := &supervisor{
		stateDir: stateDir,
		stdout:   stdout,
		stderr:   stderr,
		state: &jobRecord{
			Version:    protocolVersion,
			JobID:      "job-1",
			SpecSHA256: strings.Repeat("a", 64),
			State:      "running",
			Sequence:   2,
		},
	}
	s.watchTruncation(stdout)
	s.watchTruncation(stderr)
	s.mu.Lock()
	if err := s.persistStateLocked(); err != nil {
		t.Fatal(err)
	}

	// The supervisor lock is held, as start holds it across cmd.Wait on its
	// failure path: the output copier must still complete its write.
	written := make(chan error, 1)
	go func() {
		_, writeErr := stdout.Write([]byte("123456"))
		written <- writeErr
	}()
	select {
	case writeErr := <-written:
		if writeErr != nil {
			t.Fatal(writeErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("truncating write waited on the supervisor lock")
	}
	if persisted := readPersistedState(t, stateDir); persisted.StdoutTruncated || persisted.Sequence != 2 {
		t.Fatalf("truncation persisted under a held supervisor lock: %#v", persisted)
	}
	s.mu.Unlock()

	persisted := waitForPersistedState(t, stateDir, func(record jobRecord) bool {
		return record.StdoutTruncated
	})
	if persisted.Sequence != 3 || persisted.StderrTruncated || persisted.State != "running" {
		t.Fatalf("stdout truncation was not persisted alone: %#v", persisted)
	}

	if _, err := stdout.Write([]byte("more")); err != nil {
		t.Fatal(err)
	}
	if _, err := stderr.Write([]byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	persisted = waitForPersistedState(t, stateDir, func(record jobRecord) bool {
		return record.StderrTruncated
	})
	if persisted.Sequence != 4 || !persisted.StdoutTruncated {
		t.Fatalf("stderr truncation was not persisted once: %#v", persisted)
	}
	if _, err := stderr.Write([]byte("again")); err != nil {
		t.Fatal(err)
	}
	// A later write never repeats the notification; a direct call is a no-op.
	s.persistTruncation(stderr)
	if persisted := readPersistedState(t, stateDir); persisted.Sequence != 4 {
		t.Fatalf("repeated truncation advanced the sequence: %#v", persisted)
	}
}

func TestTruncationAfterTerminalStateIsNotPersisted(t *testing.T) {
	stateDir := t.TempDir()
	stdout, err := newBoundedWriter(filepath.Join(stateDir, "stdout.log"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stdout.close)
	code := 0
	s := &supervisor{
		stateDir: stateDir,
		stdout:   stdout,
		state: &jobRecord{
			Version:    protocolVersion,
			JobID:      "job-1",
			SpecSHA256: strings.Repeat("a", 64),
			State:      "exited",
			ExitCode:   &code,
			Sequence:   3,
		},
	}
	s.mu.Lock()
	if err := s.persistStateLocked(); err != nil {
		t.Fatal(err)
	}
	s.mu.Unlock()
	s.persistTruncation(stdout)
	if persisted := readPersistedState(t, stateDir); persisted.Sequence != 3 || persisted.StdoutTruncated {
		t.Fatalf("a terminal record was rewritten: %#v", persisted)
	}
}

func TestSupervisorPersistsTruncationWhileProcessRuns(t *testing.T) {
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

	started := callSupervisor(t, socket, request{
		Version:        protocolVersion,
		Action:         "start",
		JobID:          "job-1",
		Argv:           []string{"sh", "-c", "printf 123456789; printf more; printf err >&2; exec sleep 30"},
		Env:            map[string]string{"PATH": "/bin:/usr/bin"},
		Cwd:            "/",
		UID:            uint32(os.Getuid()),
		GID:            uint32(os.Getgid()),
		MaxStdoutBytes: 5,
		MaxStderrBytes: 8,
	})
	if !started.OK || started.Job == nil {
		t.Fatalf("unexpected start response: %#v", started)
	}
	persisted := waitForPersistedState(t, stateDir, func(record jobRecord) bool {
		return record.StdoutTruncated
	})
	// Admission, running, then exactly one truncation.
	if persisted.State != "running" || persisted.Sequence != 3 || persisted.StderrTruncated {
		t.Fatalf("running truncation was not persisted once: %#v", persisted)
	}

	signaled := callSupervisor(t, socket, request{Version: protocolVersion, Action: "signal", JobID: "job-1", Signal: 15})
	if !signaled.OK {
		t.Fatalf("signal failed: %#v", signaled)
	}
	persisted = waitForPersistedState(t, stateDir, func(record jobRecord) bool {
		return record.State != "running"
	})
	if persisted.State != "signaled" || persisted.Sequence != 4 || !persisted.StdoutTruncated ||
		persisted.StderrTruncated || persisted.StdoutBytes != 5 || persisted.StderrBytes != 3 {
		t.Fatalf("unexpected terminal record: %#v", persisted)
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

func TestControlReportsOnlyDialFailureAsBeforeDispatch(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "ctl", "--socket", filepath.Join(t.TempDir(), "absent.sock"))
	command.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	command.Stdin = strings.NewReader(`{"version":1,"action":"start","job_id":"job"}`)
	output, err := command.Output()
	if err == nil {
		t.Fatal("missing control socket unexpectedly succeeded")
	}
	var reply map[string]any
	if err := json.Unmarshal(output, &reply); err != nil {
		t.Fatalf("missing typed before-dispatch response: %s: %v", output, err)
	}
	if reply["ok"] != false || reply["error_code"] != "control_not_connected" {
		t.Fatalf("wrong before-dispatch classification: %#v", reply)
	}
}
