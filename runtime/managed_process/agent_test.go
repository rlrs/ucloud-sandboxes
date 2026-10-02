//go:build linux

package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// testNode is the Warden side of one agent connection.
type testNode struct {
	t      *testing.T
	conn   net.Conn
	reader *bufio.Reader
	hello  map[string]any
	served chan bool
}

type agentHarness struct {
	t        *testing.T
	agent    *agent
	listener net.Listener
	socket   string
}

func newAgentHarness(t *testing.T, pingInterval time.Duration) *agentHarness {
	t.Helper()
	// Socket paths are limited to 108 bytes; t.TempDir() can exceed that.
	dir, err := os.MkdirTemp("/tmp", "ucga-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	environment := append(os.Environ(), "GO_WANT_HELPER_PROCESS=1", agentEnvPrefix+"SOCKET="+socket)
	return &agentHarness{t: t, agent: newAgent([]string{self, "files"}, environment, pingInterval), listener: listener, socket: socket}
}

func (h *agentHarness) connect(window int64) *testNode {
	h.t.Helper()
	served := make(chan bool, 1)
	go func() {
		conn, err := net.Dial("unix", h.socket)
		if err != nil {
			served <- false
			return
		}
		served <- h.agent.serve(conn)
	}()
	conn, err := h.listener.Accept()
	if err != nil {
		h.t.Fatal(err)
	}
	node := &testNode{t: h.t, conn: conn, reader: bufio.NewReader(conn), served: served}
	h.t.Cleanup(func() { node.close() })
	node.send(map[string]any{"type": "hello", "version": 1, "window": window}, nil)
	node.hello, _ = node.expect("hello")
	return node
}

func (n *testNode) close() bool {
	_ = n.conn.Close()
	select {
	case established := <-n.served:
		n.served <- established
		return established
	case <-time.After(5 * time.Second):
		n.t.Fatal("agent did not end the connection")
		return false
	}
}

func (n *testNode) sendRaw(header, payload []byte) {
	n.t.Helper()
	prefix := make([]byte, 8)
	binary.BigEndian.PutUint32(prefix[:4], uint32(len(header)))
	binary.BigEndian.PutUint32(prefix[4:], uint32(len(payload)))
	if _, err := n.conn.Write(append(append(prefix, header...), payload...)); err != nil {
		n.t.Fatal(err)
	}
}

func (n *testNode) send(header map[string]any, payload []byte) {
	n.t.Helper()
	encoded, err := json.Marshal(header)
	if err != nil {
		n.t.Fatal(err)
	}
	n.sendRaw(encoded, payload)
}

func (n *testNode) recv(timeout time.Duration) (map[string]any, []byte, error) {
	_ = n.conn.SetReadDeadline(time.Now().Add(timeout))
	header, payload, err := readFrame(n.reader)
	if err != nil {
		return nil, nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(header))
	decoder.UseNumber()
	var decoded map[string]any
	if err := decoder.Decode(&decoded); err != nil {
		n.t.Fatalf("agent sent an invalid header %q: %v", header, err)
	}
	return decoded, payload, nil
}

func (n *testNode) expect(kind string) (map[string]any, []byte) {
	n.t.Helper()
	header, payload, err := n.recv(10 * time.Second)
	if err != nil {
		n.t.Fatalf("waiting for %s: %v", kind, err)
	}
	if header["type"] != kind {
		n.t.Fatalf("expected %s, got %v", kind, header)
	}
	return header, payload
}

func (n *testNode) expectClosed() {
	n.t.Helper()
	for {
		header, _, err := n.recv(5 * time.Second)
		if errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) {
			return
		}
		if err != nil {
			n.t.Fatalf("agent kept the connection open: %v", err)
		}
		if header["type"] == "ping" {
			continue
		}
	}
}

// collect reads one op's frames to its terminal frame, crediting output as it
// arrives, and returns the terminal header and the output per stream.
func (n *testNode) collect(id int) (map[string]any, map[string][]byte) {
	n.t.Helper()
	output := map[string][]byte{}
	for {
		header, payload := n.expectAny()
		if number(header["id"]) != id {
			n.t.Fatalf("frame for another op: %v", header)
		}
		switch header["type"] {
		case "started", "input_credit":
		case "output":
			stream := header["stream"].(string)
			output[stream] = append(output[stream], payload...)
			n.send(map[string]any{"type": "credit", "id": id, "stream": stream, "offset": len(output[stream])}, nil)
		case "exit", "done", "error":
			return header, output
		default:
			n.t.Fatalf("unexpected frame %v", header)
		}
	}
}

func (n *testNode) expectAny() (map[string]any, []byte) {
	n.t.Helper()
	for {
		header, payload, err := n.recv(10 * time.Second)
		if err != nil {
			n.t.Fatal(err)
		}
		if header["type"] == "ping" {
			n.send(map[string]any{"type": "pong"}, nil)
			continue
		}
		return header, payload
	}
}

func number(value any) int {
	if value == nil {
		return -1
	}
	parsed, err := strconv.Atoi(string(value.(json.Number)))
	if err != nil {
		panic(err)
	}
	return parsed
}

func execFrame(id int, stdin bool, argv ...string) map[string]any {
	return map[string]any{
		"type": "exec", "id": id, "argv": argv, "env": map[string]string{"EXTRA": "1"},
		"cwd": "/tmp", "uid": os.Geteuid(), "gid": os.Getegid(), "stdin": stdin,
	}
}

func fileFrame(kind string, id int, path string, maxBytes int) map[string]any {
	frame := map[string]any{"type": kind, "id": id, "path": path, "uid": os.Geteuid(), "gid": os.Getegid()}
	if kind != "stat" {
		frame["max_bytes"] = maxBytes
	}
	return frame
}

func waitGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			t.Fatalf("process %d survived", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAgentStreamsBinaryOutputEnvironmentAndExitStatus(t *testing.T) {
	node := newAgentHarness(t, time.Minute).connect(minFlowWindow)
	if node.hello["build"] != agentBuild || number(node.hello["abandoned"]) != 0 || len(node.hello["agent"].(string)) != 32 {
		t.Fatalf("unexpected hello %v", node.hello)
	}
	node.send(execFrame(1, false, "/bin/sh", "-c",
		`printf '\000\377\n'; printf '%s:%s:%s' "${UCLOUD_GUEST_AGENT_SOCKET-unset}" "$EXTRA" "$PWD" >&2; exit 3`), nil)
	started, _ := node.expect("started")
	if number(started["pid"]) <= 0 {
		t.Fatalf("started without a pid: %v", started)
	}
	exit, output := node.collect(1)
	if !bytes.Equal(output["stdout"], []byte{0, 0xff, '\n'}) || string(output["stderr"]) != "unset:1:/tmp" {
		t.Fatalf("output changed: %q", output)
	}
	if number(exit["exit_code"]) != 3 || exit["signal"] != nil || number(exit["stdout_bytes"]) != 3 ||
		number(exit["stderr_bytes"]) != 12 || exit["output_complete"] != true {
		t.Fatalf("unexpected exit %v", exit)
	}
	node.send(execFrame(2, false, "/nonexistent/binary"), nil)
	failure, _ := node.expect("error")
	if number(failure["id"]) != 2 || failure["code"] != "spawn_failed" {
		t.Fatalf("unexpected spawn failure %v", failure)
	}
	node.send(map[string]any{
		"type": "exec", "id": 3, "argv": []string{"true"}, "env": map[string]string{"BAD-KEY": "x"},
		"cwd": "/", "uid": os.Geteuid(), "gid": os.Getegid(), "stdin": false,
	}, nil)
	if invalid, _ := node.expect("error"); invalid["code"] != "invalid" {
		t.Fatalf("invalid environment was accepted: %v", invalid)
	}
	// Go escapes '<' as <: quoting this argv[0] unbounded would make a
	// 1.2 MB error header, which the node must treat as a violation.
	uid, gid := strconv.Itoa(os.Geteuid()), strconv.Itoa(os.Getegid())
	node.sendRaw([]byte(`{"type":"exec","id":4,"argv":["`+strings.Repeat("<", 200_000)+
		`"],"env":{},"cwd":"/","uid":`+uid+`,"gid":`+gid+`,"stdin":false}`), nil)
	if failure, _ := node.expect("error"); failure["code"] != "spawn_failed" || len(failure["message"].(string)) > maxCapturedBytes {
		t.Fatalf("unbounded spawn failure %.200v", failure)
	}
	node.send(execFrame(5, false, "true"), nil)
	if exit, _ := node.collect(5); number(exit["exit_code"]) != 0 {
		t.Fatalf("the connection did not survive a long argv: %v", exit)
	}
}

func TestAgentOutputWaitsForCredit(t *testing.T) {
	node := newAgentHarness(t, time.Minute).connect(minFlowWindow)
	const total = 1 << 20
	node.send(execFrame(1, false, "head", "-c", strconv.Itoa(total), "/dev/zero"), nil)
	node.expect("started")
	received := 0
	for received < minFlowWindow {
		header, payload := node.expect("output")
		if header["stream"] != "stdout" {
			t.Fatalf("unexpected stream %v", header)
		}
		received += len(payload)
	}
	if received != minFlowWindow {
		t.Fatalf("agent sent %d bytes against a %d byte window", received, minFlowWindow)
	}
	if header, _, err := node.recv(300 * time.Millisecond); err == nil {
		t.Fatalf("agent sent beyond its credit: %v", header)
	}
	credited := 0
	for {
		if received-credited >= minFlowWindow/4 {
			credited = received
			node.send(map[string]any{"type": "credit", "id": 1, "stream": "stdout", "offset": credited}, nil)
		}
		header, payload := node.expectAny()
		if header["type"] == "exit" {
			if number(header["stdout_bytes"]) != total || received != total {
				t.Fatalf("exit after %d bytes: %v", received, header)
			}
			return
		}
		received += len(payload)
		if received-credited > minFlowWindow {
			t.Fatalf("agent exceeded its window: %d outstanding", received-credited)
		}
	}
}

// The leader exits at once; a descendant's output fills the socket of a node
// that stalls past the idle grace. Waiting on the node is not idleness.
func TestAgentAStalledNodeIsNotIdleOutput(t *testing.T) {
	t.Parallel()
	node := newAgentHarness(t, time.Minute).connect(4 << 20)
	const total = 8 << 20
	node.send(execFrame(1, false, "/bin/sh", "-c", "head -c "+strconv.Itoa(total)+" /dev/zero & exit 0"), nil)
	node.expect("started")
	time.Sleep(outputIdleGrace + 500*time.Millisecond)
	exit, output := node.collect(1)
	if exit["output_complete"] != true || len(output["stdout"]) != total {
		t.Fatalf("a stalled node truncated the output: %v after %d of %d bytes", exit, len(output["stdout"]), total)
	}
}

func TestAgentStdinIsCreditedAndClosed(t *testing.T) {
	node := newAgentHarness(t, time.Minute).connect(minFlowWindow)
	node.send(execFrame(1, true, "cat"), nil)
	node.expect("started")
	chunk := bytes.Repeat([]byte{0, 1, 2, 0xff}, minFlowWindow/16)
	for range 3 {
		node.send(map[string]any{"type": "input", "id": 1}, chunk)
	}
	node.send(map[string]any{"type": "input_close", "id": 1}, nil)
	credit := 0
	output := []byte{}
	for {
		header, payload := node.expectAny()
		switch header["type"] {
		case "input_credit":
			credit = number(header["offset"])
		case "output":
			output = append(output, payload...)
		case "exit":
			if number(header["exit_code"]) != 0 || !bytes.Equal(output, bytes.Repeat(chunk, 3)) {
				t.Fatalf("stdin round trip failed: %v, %d bytes", header, len(output))
			}
			// cat sees EOF only after the feeder credited every chunk.
			if credit != 3*len(chunk) {
				t.Fatalf("input was credited only to %d", credit)
			}
			return
		}
	}
}

func TestAgentInputBeyondCreditIsAViolation(t *testing.T) {
	op := &agentOp{conn: &agentConn{window: minFlowWindow}, takesInput: true}
	op.cond = sync.NewCond(&op.mu)
	if err := op.addInput(make([]byte, minFlowWindow)); err != nil {
		t.Fatal(err)
	}
	if err := op.addInput([]byte{1}); !errors.Is(err, errProtocol) {
		t.Fatalf("input beyond credit was accepted: %v", err)
	}
}

func TestAgentSignalsTheProcessGroup(t *testing.T) {
	node := newAgentHarness(t, time.Minute).connect(minFlowWindow)
	node.send(execFrame(1, false, "/bin/sh", "-c", "sleep 30 & echo $!; wait"), nil)
	node.expect("started")
	header, payload := node.expect("output")
	child, err := strconv.Atoi(strings.TrimSpace(string(payload)))
	if err != nil || header["stream"] != "stdout" {
		t.Fatalf("no child pid: %v %q", header, payload)
	}
	node.send(map[string]any{"type": "signal", "id": 1, "signal": 15}, nil)
	exit, _ := node.collect(1)
	if exit["exit_code"] != nil || number(exit["signal"]) != 15 {
		t.Fatalf("unexpected exit %v", exit)
	}
	waitGone(t, child)
}

func TestAgentReportsOutputHeldByADescendant(t *testing.T) {
	node := newAgentHarness(t, time.Minute).connect(minFlowWindow)
	node.send(execFrame(1, false, "/bin/sh", "-c", "(sleep 30 &); echo hi"), nil)
	started, _ := node.expect("started")
	exit, output := node.collect(1)
	_ = syscall.Kill(-number(started["pid"]), syscall.SIGKILL)
	if number(exit["exit_code"]) != 0 || exit["output_complete"] != false || string(output["stdout"]) != "hi\n" {
		t.Fatalf("unexpected exit %v %q", exit, output)
	}
}

func TestAgentFileOperations(t *testing.T) {
	node := newAgentHarness(t, time.Minute).connect(minFlowWindow)
	dir := t.TempDir()
	target := filepath.Join(dir, "nested", "file")
	content := []byte("hello\x00world\xff")
	node.send(fileFrame("write_file", 1, target, len(content)), nil)
	node.send(map[string]any{"type": "input", "id": 1}, content)
	node.send(map[string]any{"type": "input_close", "id": 1}, nil)
	if done, _ := node.collect(1); done["type"] != "done" || done["stat"] != nil {
		t.Fatalf("write failed: %v", done)
	}
	node.send(fileFrame("read_file", 2, target, len(content)), nil)
	if done, output := node.collect(2); done["type"] != "done" || !bytes.Equal(output["stdout"], content) {
		t.Fatalf("read failed: %v %q", done, output)
	}
	node.send(fileFrame("stat", 3, target, 0), nil)
	done, _ := node.collect(3)
	stat, _ := done["stat"].(map[string]any)
	if done["type"] != "done" || stat["type"] != "file" || number(stat["size"]) != len(content) || number(stat["mode"]) != 0o600 {
		t.Fatalf("stat failed: %v", done)
	}
	node.send(fileFrame("stat", 4, dir, 0), nil)
	if done, _ := node.collect(4); done["stat"].(map[string]any)["type"] != "directory" {
		t.Fatalf("directory stat failed: %v", done)
	}
	for id, test := range []struct {
		frame map[string]any
		code  string
	}{
		{fileFrame("read_file", 0, filepath.Join(dir, "missing"), 8), "not_found"},
		{fileFrame("read_file", 0, target, 4), "too_large"},
		{fileFrame("read_file", 0, dir, 8), "not_regular"},
		{fileFrame("read_file", 0, "relative", 8), "invalid"},
		{fileFrame("write_file", 0, dir, 8), "not_regular"},
	} {
		test.frame["id"] = id + 5
		node.send(test.frame, nil)
		if test.frame["type"] == "write_file" {
			node.send(map[string]any{"type": "input_close", "id": id + 5}, nil)
		}
		if failure, _ := node.collect(id + 5); failure["type"] != "error" || failure["code"] != test.code {
			t.Fatalf("%v: expected %s, got %v", test.frame, test.code, failure)
		}
	}
	// A panicking or out-of-memory helper exits 2: never the caller's mistake.
	if code, mapped := fileErrorCodes[2]; mapped {
		t.Fatalf("exit status 2 maps to %q", code)
	}
	if os.Geteuid() != 0 {
		frame := fileFrame("stat", 20, target, 0)
		frame["uid"] = os.Geteuid() + 1
		node.send(frame, nil)
		if failure, _ := node.collect(20); failure["code"] != "credentials" {
			t.Fatalf("credential change was accepted: %v", failure)
		}
	}
}

func TestAgentConnectionLossKillsOps(t *testing.T) {
	harness := newAgentHarness(t, time.Minute)
	node := harness.connect(minFlowWindow)
	node.send(execFrame(1, false, "sleep", "30"), nil)
	started, _ := node.expect("started")
	if !node.close() {
		t.Fatal("handshake was not reported as established")
	}
	waitGone(t, number(started["pid"]))
	next := harness.connect(minFlowWindow)
	if number(next.hello["abandoned"]) != 1 || next.hello["agent"] != node.hello["agent"] {
		t.Fatalf("reconnect hello %v after %v", next.hello, node.hello)
	}
	next.send(execFrame(1, false, "true"), nil)
	if exit, _ := next.collect(1); number(exit["exit_code"]) != 0 {
		t.Fatalf("new connection did not serve: %v", exit)
	}
}

func TestAgentRejectsProtocolViolations(t *testing.T) {
	uid, gid := strconv.Itoa(os.Geteuid()), strconv.Itoa(os.Getegid())
	sleepExec := `{"type":"exec","id":1,"argv":["sleep","5"],"env":{},"cwd":"/","uid":` + uid + `,"gid":` + gid + `,"stdin":false}`
	cases := map[string][]string{
		"unknown type":   {`{"type":"nope"}`},
		"extra key":      {`{"type":"pong","extra":1}`},
		"null value":     {`{"type":"signal","id":null,"signal":1}`},
		"mistyped value": {sleepExec, `{"type":"signal","id":"1","signal":1}`},
		"trailing bytes": {`{"type":"pong"} `},
		"second hello":   {`{"type":"hello","version":1,"window":65536}`},
		"unissued op":    {`{"type":"credit","id":5,"stream":"stdout","offset":0}`},
		"reused op id":   {sleepExec, sleepExec},
		"credit beyond":  {sleepExec, `{"type":"credit","id":1,"stream":"stdout","offset":1}`},
		// The helper waits for input that never closes, so the op stays live.
		"credit captured": {`{"type":"write_file","id":1,"path":"/tmp/ucga-never","max_bytes":8,"uid":` + uid + `,"gid":` + gid + `}`, `{"type":"credit","id":1,"stream":"stdout","offset":0}`},
		"input without":   {sleepExec, `{"type":"input","id":1}`},
	}
	for name, headers := range cases {
		t.Run(name, func(t *testing.T) {
			node := newAgentHarness(t, time.Minute).connect(minFlowWindow)
			for index, header := range headers {
				var payload []byte
				if index == len(headers)-1 && strings.Contains(header, `"input"`) {
					payload = []byte("x")
				}
				node.sendRaw([]byte(header), payload)
			}
			node.expectClosed()
		})
	}
	t.Run("payload on a control frame", func(t *testing.T) {
		node := newAgentHarness(t, time.Minute).connect(minFlowWindow)
		node.sendRaw([]byte(`{"type":"pong"}`), []byte("x"))
		node.expectClosed()
	})
	t.Run("oversized frame", func(t *testing.T) {
		node := newAgentHarness(t, time.Minute).connect(minFlowWindow)
		prefix := make([]byte, 8)
		binary.BigEndian.PutUint32(prefix[:4], maxFrameHeaderBytes+1)
		if _, err := node.conn.Write(prefix); err != nil {
			t.Fatal(err)
		}
		node.expectClosed()
	})
	t.Run("wrong version", func(t *testing.T) {
		harness := newAgentHarness(t, time.Minute)
		served := make(chan bool, 1)
		go func() {
			conn, _ := net.Dial("unix", harness.socket)
			served <- harness.agent.serve(conn)
		}()
		conn, err := harness.listener.Accept()
		if err != nil {
			t.Fatal(err)
		}
		node := &testNode{t: t, conn: conn, reader: bufio.NewReader(conn), served: served}
		node.send(map[string]any{"type": "hello", "version": 2, "window": minFlowWindow}, nil)
		node.expectClosed()
		if <-served {
			t.Fatal("a rejected handshake was reported as established")
		}
	})
}

func TestAgentLivenessProbe(t *testing.T) {
	harness := newAgentHarness(t, 100*time.Millisecond)
	silent := harness.connect(minFlowWindow)
	silent.expect("ping")
	started := time.Now()
	silent.expectClosed()
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("unanswered probe took %v to end the connection", elapsed)
	}
	answering := harness.connect(minFlowWindow)
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		header, _, err := answering.recv(time.Until(deadline))
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				break
			}
			t.Fatalf("answered probes still ended the connection: %v", err)
		}
		if header["type"] == "ping" {
			answering.send(map[string]any{"type": "pong"}, nil)
		}
	}
	answering.send(execFrame(1, false, "true"), nil)
	if exit, _ := answering.collect(1); number(exit["exit_code"]) != 0 {
		t.Fatalf("unexpected exit %v", exit)
	}
}

// A node that stops reading blocks the agent's writers; the probe must still
// end the connection rather than wait behind them.
func TestAgentProbeTimesOutWhileItsWritesBlock(t *testing.T) {
	node := newAgentHarness(t, 100*time.Millisecond).connect(4 << 20)
	node.send(execFrame(1, false, "head", "-c", strconv.Itoa(8<<20), "/dev/zero"), nil)
	select {
	case established := <-node.served:
		node.served <- established
	case <-time.After(5 * time.Second):
		t.Fatal("the agent kept a connection whose node stopped reading")
	}
}

// startAgentProcess runs the agent as its own process, which a test can
// freeze as runsc pause does, and completes its handshake.
func startAgentProcess(t *testing.T, harness *agentHarness, flags ...string) (*os.Process, *testNode) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	process := exec.Command(self, append([]string{"agent", "--connect", harness.socket}, flags...)...)
	process.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = process.Process.Signal(syscall.SIGCONT)
		_ = process.Process.Kill()
		_ = process.Wait()
	})
	conn, err := harness.listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	node := &testNode{t: t, conn: conn, reader: bufio.NewReader(conn)}
	t.Cleanup(func() { _ = conn.Close() })
	node.send(map[string]any{"type": "hello", "version": 1, "window": minFlowWindow}, nil)
	node.expect("hello")
	return process.Process, node
}

func freeze(t *testing.T, process *os.Process, duration time.Duration) {
	t.Helper()
	if err := process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	time.Sleep(duration)
	if err := process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
}

// A freeze longer than the idle grace while a descendant still holds the
// output must not read as idleness: the thawed pump may not have run yet.
func TestAgentAFreezeDuringTheDrainKeepsOutput(t *testing.T) {
	t.Parallel()
	process, node := startAgentProcess(t, newAgentHarness(t, time.Minute))
	node.send(execFrame(1, false, "/bin/sh", "-c", "(sleep 0.3; echo late) & echo early"), nil)
	node.expect("started")
	time.Sleep(100 * time.Millisecond) // the leader has exited
	freeze(t, process, outputIdleGrace+500*time.Millisecond)
	exit, output := node.collect(1)
	if exit["output_complete"] != true || string(output["stdout"]) != "early\nlate\n" {
		t.Fatalf("a freeze during the drain cut the output: %v %q", exit, output)
	}
}

// runsc pause freezes the agent; one that resumes with a probe outstanding
// must probe again rather than count the frozen time against the node.
func TestAgentSurvivesAFreezeWithAProbeOutstanding(t *testing.T) {
	process, node := startAgentProcess(t, newAgentHarness(t, time.Minute), "--ping-interval=200ms")
	node.expect("ping")
	freeze(t, process, time.Second)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		header, _, err := node.recv(time.Until(deadline))
		if errors.Is(err, os.ErrDeadlineExceeded) {
			break
		}
		if err != nil {
			t.Fatalf("the agent dropped the connection after a freeze: %v", err)
		}
		if header["type"] == "ping" {
			node.send(map[string]any{"type": "pong"}, nil)
		}
	}
	node.send(execFrame(1, false, "true"), nil)
	if exit, _ := node.collect(1); number(exit["exit_code"]) != 0 {
		t.Fatalf("unexpected exit %v", exit)
	}
}

func FuzzAgentFrameDecoding(f *testing.F) {
	f.Add([]byte(`{"type":"exec","id":1,"argv":["true"],"env":{},"cwd":"/","uid":0,"gid":0,"stdin":false}`), []byte{})
	f.Add([]byte(`{"type":"input","id":1}`), []byte("data"))
	f.Add([]byte(`{"type":"credit","id":1,"stream":"stdout","offset":-1}`), []byte{})
	f.Add([]byte(`null`), []byte{})
	f.Fuzz(func(t *testing.T, header, payload []byte) {
		frame, err := decodeInbound(header, payload)
		if err != nil {
			if !errors.Is(err, errProtocol) {
				t.Fatalf("decode failed without a protocol error: %v", err)
			}
			return
		}
		if _, known := agentInboundKeys[frame.Type]; !known {
			t.Fatalf("accepted unknown type %q", frame.Type)
		}
		if (frame.Type == "input") != (len(payload) > 0) {
			t.Fatalf("accepted %q with a %d byte payload", frame.Type, len(payload))
		}
		var prefix [8]byte
		binary.BigEndian.PutUint32(prefix[:4], uint32(len(header)))
		binary.BigEndian.PutUint32(prefix[4:], uint32(len(payload)))
		readHeader, readPayload, err := readFrame(bytes.NewReader(append(append(prefix[:], header...), payload...)))
		if len(header) <= maxFrameHeaderBytes && len(payload) <= maxFramePayloadBytes &&
			(err != nil || !bytes.Equal(readHeader, header) || !bytes.Equal(readPayload, payload)) {
			t.Fatalf("frame did not round-trip: %v", err)
		}
	})
}
