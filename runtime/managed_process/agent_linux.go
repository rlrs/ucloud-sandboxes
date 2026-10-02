//go:build linux

package main

// Guest agent, protocol version 1. docs/guest-agent-protocol.md gives the
// rationale; this comment is the wire contract.
//
// Transport. The agent dials a Warden-owned Unix stream socket in a directory
// bind-mounted into the guest (runsc --host-uds=open). The guest never listens
// on a host-visible socket: a host-bound guest listener makes every checkpoint
// fail and destroys the sandbox (qualification 2026-10-02, S7).
//
// Frame. An 8-byte prefix holds the header length H (1..1 MiB) and the payload
// length P (0..1 MiB), both uint32 big-endian. H bytes of one JSON object, the
// header, follow, then P raw payload bytes. Only input (node to agent) and
// output (agent to node) carry a payload, and theirs is never empty. A header
// has exactly the keys listed for its type plus "type", none null, and no
// bytes after the object. Anything else is a protocol violation: the receiver
// closes the connection.
//
// Handshake. The node speaks first: hello{version, window}. The agent replies
// hello{version, agent, pid, build, abandoned}. window, 64 KiB..64 MiB, bounds
// every flow below. Nothing precedes a side's hello; a second one violates.
//
// Node to agent:
//
//	exec{id, argv, env, cwd, uid, gid, stdin}
//	read_file{id, path, max_bytes, uid, gid}
//	write_file{id, path, max_bytes, uid, gid}   content follows as input
//	stat{id, path, uid, gid}
//	input{id} + payload                        exec stdin or file content
//	input_close{id}
//	signal{id, signal}                         1..64, to the op's process group
//	credit{id, stream, offset}                 output bytes consumed, cumulative
//	pong{}
//
// Agent to node:
//
//	started{id, pid}                           exec only, before its output
//	output{id, stream} + payload               stdout|stderr; read_file data is stdout
//	input_credit{id, offset}                   input bytes consumed, cumulative
//	exit{id, exit_code, signal, stdout_bytes, stderr_bytes, output_complete}
//	done{id, stat}                             file ops; stat is null except for stat
//	error{id, code, message}
//	ping{}
//
// exit, done and error are terminal: an op's last frame. error means the op
// did not start, failed inside the agent, or, for a file op, failed.
//
// Ops. Ids are 1..2^53-1 and strictly increase per connection. A frame for an
// id no larger than the largest issued that is no longer live is ignored: it
// raced the op's end. A frame for a larger id violates.
//
// Flow control. Per op and output stream, bytes sent minus the credit offset
// never exceed window; per op, input bytes sent minus input_credit never
// exceed window. A receiver credits once it has consumed window/4 beyond its
// last credit. Senders send what fits, so a blocked sender's receiver holds
// window unconsumed bytes and will credit.
//
// Termination. After an exec's process exits the agent waits for EOF on both
// outputs. If both stay idle for 2 s, not counting time blocked on the node
// (credit or a send) or frozen, a descendant holds them: the agent closes its
// ends and reports output_complete false. exit follows all output. exit_code
// is null exactly when signal is set.
//
// Connection loss. Ops never outlive their connection. When it ends (EOF,
// error, violation, liveness timeout) the agent SIGKILLs the process group of
// every live op, discards its output and reports the count as abandoned in the
// next hello; the node fails every op that has no terminal frame. Nothing is
// resumed. runsc pause keeps the connection: ops stall, then continue.
//
// Liveness. The agent pings after ping-interval without receiving a frame and
// redials if nothing arrives within another interval, even while its own
// writes are blocked. Any frame answers a ping: the node sends pong unless it
// already has bytes queued for the agent. A late tick means the guest was
// frozen, so the probe restarts instead of failing. The node never times out
// the connection.
//
// Reconnect. The agent redials at once, then backs off from backoff-min,
// doubling to backoff-max, with ±25% jitter.

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	mathrand "math/rand"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const (
	agentProtocolVersion = 1
	agentBuild           = "guest-agent-v1"
	maxFrameHeaderBytes  = 1 << 20
	maxFramePayloadBytes = 1 << 20
	minFlowWindow        = 64 << 10
	maxFlowWindow        = 64 << 20
	// One default pipe buffer: a read never returns more.
	maxOutputChunk   = 64 << 10
	maxAgentOps      = 1024
	maxCapturedBytes = 4096
	maxOpID          = 1<<53 - 1
	outputIdleGrace  = 2 * time.Second
	helloTimeout     = 30 * time.Second
	agentEnvPrefix   = "UCLOUD_GUEST_AGENT_"
)

var (
	errProtocol = errors.New("guest agent protocol violation")
	streamNames = [2]string{"stdout", "stderr"}
	// Exit statuses of the files helper (files.go) as protocol error codes.
	fileErrorCodes = map[int]string{
		fileExitInvalid:    "invalid",
		fileExitNotFound:   "not_found",
		fileExitPermission: "permission_denied",
		fileExitTooLarge:   "too_large",
		fileExitNotRegular: "not_regular",
	}
	// Header keys, besides "type", of every frame the agent accepts.
	agentInboundKeys = map[string][]string{
		"hello":       {"version", "window"},
		"exec":        {"id", "argv", "env", "cwd", "uid", "gid", "stdin"},
		"read_file":   {"id", "path", "max_bytes", "uid", "gid"},
		"write_file":  {"id", "path", "max_bytes", "uid", "gid"},
		"stat":        {"id", "path", "uid", "gid"},
		"input":       {"id"},
		"input_close": {"id"},
		"signal":      {"id", "signal"},
		"credit":      {"id", "stream", "offset"},
		"pong":        {},
	}
)

func violation(format string, values ...any) error {
	return fmt.Errorf("%w: %s", errProtocol, fmt.Sprintf(format, values...))
}

type inboundFrame struct {
	Type     string            `json:"type"`
	Version  int               `json:"version"`
	Window   int64             `json:"window"`
	ID       uint64            `json:"id"`
	Argv     []string          `json:"argv"`
	Env      map[string]string `json:"env"`
	Cwd      string            `json:"cwd"`
	UID      uint32            `json:"uid"`
	GID      uint32            `json:"gid"`
	Stdin    bool              `json:"stdin"`
	Path     string            `json:"path"`
	MaxBytes int64             `json:"max_bytes"`
	Signal   int               `json:"signal"`
	Stream   string            `json:"stream"`
	Offset   int64             `json:"offset"`
}

type helloFrame struct {
	Type      string `json:"type"`
	Version   int    `json:"version"`
	Agent     string `json:"agent"`
	PID       int    `json:"pid"`
	Build     string `json:"build"`
	Abandoned int64  `json:"abandoned"`
}

type startedFrame struct {
	Type string `json:"type"`
	ID   uint64 `json:"id"`
	PID  int    `json:"pid"`
}

type outputFrame struct {
	Type   string `json:"type"`
	ID     uint64 `json:"id"`
	Stream string `json:"stream"`
}

type inputCreditFrame struct {
	Type   string `json:"type"`
	ID     uint64 `json:"id"`
	Offset int64  `json:"offset"`
}

type exitFrame struct {
	Type           string `json:"type"`
	ID             uint64 `json:"id"`
	ExitCode       *int   `json:"exit_code"`
	Signal         *int   `json:"signal"`
	StdoutBytes    int64  `json:"stdout_bytes"`
	StderrBytes    int64  `json:"stderr_bytes"`
	OutputComplete bool   `json:"output_complete"`
}

type doneFrame struct {
	Type string    `json:"type"`
	ID   uint64    `json:"id"`
	Stat *fileStat `json:"stat"`
}

type errorFrame struct {
	Type    string `json:"type"`
	ID      uint64 `json:"id"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type pingFrame struct {
	Type string `json:"type"`
}

func readFrame(reader io.Reader) ([]byte, []byte, error) {
	var prefix [8]byte
	if _, err := io.ReadFull(reader, prefix[:]); err != nil {
		return nil, nil, err
	}
	headerLength := binary.BigEndian.Uint32(prefix[:4])
	payloadLength := binary.BigEndian.Uint32(prefix[4:])
	if headerLength == 0 || headerLength > maxFrameHeaderBytes || payloadLength > maxFramePayloadBytes {
		return nil, nil, violation("frame lengths %d/%d are out of bounds", headerLength, payloadLength)
	}
	body := make([]byte, int(headerLength)+int(payloadLength))
	if _, err := io.ReadFull(reader, body); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, nil, err
	}
	return body[:headerLength], body[headerLength:], nil
}

func decodeInbound(header, payload []byte) (inboundFrame, error) {
	var fields map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(header))
	if err := decoder.Decode(&fields); err != nil || fields == nil {
		return inboundFrame{}, violation("header is not a JSON object")
	}
	if decoder.InputOffset() != int64(len(header)) {
		return inboundFrame{}, violation("header has trailing bytes")
	}
	var kind string
	if err := json.Unmarshal(fields["type"], &kind); err != nil {
		return inboundFrame{}, violation("header type is not a string")
	}
	keys, known := agentInboundKeys[kind]
	if !known || len(fields) != len(keys)+1 {
		return inboundFrame{}, violation("%q header has the wrong keys", kind)
	}
	for _, key := range keys {
		if raw, present := fields[key]; !present || string(raw) == "null" {
			return inboundFrame{}, violation("%q header lacks %q", kind, key)
		}
	}
	var frame inboundFrame
	if err := json.Unmarshal(header, &frame); err != nil {
		return inboundFrame{}, violation("%q header is mistyped: %v", kind, err)
	}
	if (kind == "input") != (len(payload) > 0) {
		return inboundFrame{}, violation("%q frame has the wrong payload", kind)
	}
	return frame, nil
}

func encodeFrame(header any, payload []byte) (net.Buffers, error) {
	encoded, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}
	head := make([]byte, 8, 8+len(encoded))
	binary.BigEndian.PutUint32(head[:4], uint32(len(encoded)))
	binary.BigEndian.PutUint32(head[4:], uint32(len(payload)))
	buffers := net.Buffers{append(head, encoded...)}
	if len(payload) > 0 {
		buffers = append(buffers, payload)
	}
	return buffers, nil
}

type agent struct {
	id           string
	helper       []string // argv prefix of the file helper: this binary, "files"
	baseEnv      []string
	pingInterval time.Duration
	abandoned    atomic.Int64

	mu       sync.Mutex
	current  *agentConn
	stopping bool
}

func newAgent(helper, environment []string, pingInterval time.Duration) *agent {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		panic(err)
	}
	baseEnv := make([]string, 0, len(environment))
	for _, item := range environment {
		if !strings.HasPrefix(item, agentEnvPrefix) {
			baseEnv = append(baseEnv, item)
		}
	}
	return &agent{id: hex.EncodeToString(random[:]), helper: helper, baseEnv: baseEnv, pingInterval: pingInterval}
}

func runAgent(arguments []string) error {
	flags := flag.NewFlagSet("agent", flag.ContinueOnError)
	socket := flags.String("connect", os.Getenv(agentEnvPrefix+"SOCKET"), "Warden socket to dial")
	pingInterval := flags.Duration("ping-interval", 10*time.Second, "liveness probe interval")
	backoffMin := flags.Duration("backoff-min", 10*time.Millisecond, "first redial delay")
	backoffMax := flags.Duration("backoff-max", 500*time.Millisecond, "largest redial delay")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 || !filepath.IsAbs(*socket) {
		return errors.New("--connect or " + agentEnvPrefix + "SOCKET must name an absolute socket path")
	}
	if *pingInterval < time.Millisecond || *backoffMin <= 0 || *backoffMax < *backoffMin {
		return errors.New("the ping interval must be at least 1ms and backoff intervals positive and ordered")
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	a := newAgent([]string{self, "files"}, os.Environ(), *pingInterval)
	go a.stopOnSignal()
	delay := *backoffMin
	for {
		// A connection that ended before its handshake backs off too: a
		// listener that rejects us must not turn this into a busy loop.
		if conn, err := net.Dial("unix", *socket); err == nil && a.serve(conn) {
			delay = *backoffMin
			continue
		}
		time.Sleep(time.Duration(float64(delay) * (0.75 + 0.5*mathrand.Float64())))
		delay = min(2*delay, *backoffMax)
	}
}

// stopOnSignal kills the live ops before exiting, as a lost connection does.
func (a *agent) stopOnSignal() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	<-signals
	a.mu.Lock()
	a.stopping = true
	current := a.current
	a.mu.Unlock()
	if current != nil {
		current.fail(errors.New("agent stopping"))
	}
	os.Exit(0)
}

// serve runs one connection to its end and reports whether its handshake
// completed.
func (a *agent) serve(conn net.Conn) bool {
	c := &agentConn{agent: a, conn: conn, ops: map[uint64]*agentOp{}, done: make(chan struct{}), epoch: time.Now()}
	a.mu.Lock()
	if a.stopping {
		a.mu.Unlock()
		_ = conn.Close()
		return false
	}
	a.current = c
	a.mu.Unlock()
	err := c.serve()
	c.fail(err)
	fmt.Fprintf(os.Stderr, "guest agent: connection ended: %v\n", c.err)
	return c.established
}

type agentConn struct {
	agent  *agent
	conn   net.Conn
	window int64
	epoch  time.Time
	// Nanoseconds after epoch; monotonic.
	lastReceived atomic.Int64
	pinging      atomic.Bool // a ping send is in flight
	maxID        uint64      // reader goroutine only
	established  bool        // reader goroutine only
	writeMu      sync.Mutex
	closed       atomic.Bool
	closeOnce    sync.Once
	done         chan struct{}
	err          error

	mu  sync.Mutex
	ops map[uint64]*agentOp
}

func (c *agentConn) serve() error {
	reader := bufio.NewReaderSize(c.conn, 256<<10)
	_ = c.conn.SetReadDeadline(time.Now().Add(helloTimeout))
	header, payload, err := readFrame(reader)
	if err != nil {
		return err
	}
	hello, err := decodeInbound(header, payload)
	if err != nil {
		return err
	}
	if hello.Type != "hello" || hello.Version != agentProtocolVersion || hello.Window < minFlowWindow || hello.Window > maxFlowWindow {
		return violation("expected a version %d hello with a valid window", agentProtocolVersion)
	}
	_ = c.conn.SetReadDeadline(time.Time{})
	c.window = hello.Window
	c.lastReceived.Store(int64(time.Since(c.epoch)))
	abandoned := c.agent.abandoned.Load()
	if err := c.send(helloFrame{"hello", agentProtocolVersion, c.agent.id, os.Getpid(), agentBuild, abandoned}, nil); err != nil {
		return err
	}
	c.agent.abandoned.Add(-abandoned)
	c.established = true
	go c.keepalive(c.agent.pingInterval)
	for {
		header, payload, err := readFrame(reader)
		if err != nil {
			return err
		}
		c.lastReceived.Store(int64(time.Since(c.epoch)))
		frame, err := decodeInbound(header, payload)
		if err == nil {
			err = c.dispatch(frame, payload)
		}
		if err != nil {
			return err
		}
	}
}

func (c *agentConn) dispatch(frame inboundFrame, payload []byte) error {
	switch frame.Type {
	case "pong":
		return nil
	case "hello":
		return violation("second hello")
	case "exec", "read_file", "write_file", "stat":
		if frame.ID == 0 || frame.ID > maxOpID || frame.ID <= c.maxID {
			return violation("op id %d does not increase", frame.ID)
		}
		c.maxID = frame.ID
		c.start(frame)
		return nil
	}
	if frame.ID == 0 || frame.ID > c.maxID {
		return violation("%s for op %d, which was never issued", frame.Type, frame.ID)
	}
	c.mu.Lock()
	op := c.ops[frame.ID]
	c.mu.Unlock()
	if op == nil {
		return nil
	}
	switch frame.Type {
	case "input":
		return op.addInput(payload)
	case "input_close":
		return op.closeInput()
	case "signal":
		if frame.Signal < 1 || frame.Signal > 64 {
			return violation("signal %d is out of range", frame.Signal)
		}
		op.signal(syscall.Signal(frame.Signal))
		return nil
	}
	return op.credit(frame.Stream, frame.Offset)
}

func (c *agentConn) send(header any, payload []byte) error {
	buffers, err := encodeFrame(header, payload)
	if err != nil {
		c.fail(err)
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closed.Load() {
		return net.ErrClosed
	}
	if _, err := buffers.WriteTo(c.conn); err != nil {
		c.fail(err)
		return err
	}
	return nil
}

// fail ends the connection once and kills every live op (see Connection loss).
func (c *agentConn) fail(err error) {
	c.closeOnce.Do(func() {
		c.err = err
		// Set before the snapshot: start registers only while this is false.
		c.closed.Store(true)
		_ = c.conn.Close()
		close(c.done)
		c.mu.Lock()
		ops := make([]*agentOp, 0, len(c.ops))
		for _, op := range c.ops {
			ops = append(ops, op)
		}
		c.mu.Unlock()
		abandoned := int64(0)
		for _, op := range ops {
			if op.cancel() {
				abandoned++
			}
		}
		c.agent.abandoned.Add(abandoned)
		c.agent.mu.Lock()
		if c.agent.current == c {
			c.agent.current = nil
		}
		c.agent.mu.Unlock()
	})
}

func (c *agentConn) keepalive(interval time.Duration) {
	tick := interval / 4
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	last := time.Since(c.epoch)
	pinged := time.Duration(-1) // no probe outstanding
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
		}
		now := time.Since(c.epoch)
		received := time.Duration(c.lastReceived.Load())
		// A tick this late means the guest was frozen: restart the probe.
		if now-last > 2*tick || received > pinged {
			pinged = -1
		}
		last = now
		if pinged < 0 {
			if now-received >= interval {
				pinged = now
				// A writer blocked on a node that stopped reading holds
				// writeMu; the probe must time out regardless, so it never
				// waits for its own ping. fail unblocks that writer.
				if c.pinging.CompareAndSwap(false, true) {
					go func() {
						_ = c.send(pingFrame{"ping"}, nil)
						c.pinging.Store(false)
					}()
				}
			}
		} else if now-pinged >= interval {
			c.fail(errors.New("liveness probe timed out"))
			return
		}
	}
}

type opSpec struct {
	argv    []string
	env     []string
	cwd     string
	cred    *syscall.Credential
	input   bool
	forward [2]bool // stdout, stderr streamed; otherwise captured
}

func (c *agentConn) start(frame inboundFrame) {
	c.mu.Lock()
	full := len(c.ops) >= maxAgentOps
	c.mu.Unlock()
	if full {
		_ = c.send(errorFrame{"error", frame.ID, "too_many_ops", "the agent has too many live ops"}, nil)
		return
	}
	spec, code, err := c.agent.opSpec(frame)
	if err == nil {
		var op *agentOp
		if op, err = c.spawn(frame.ID, frame.Type, spec); err == nil {
			c.run(op)
			return
		}
		code = "spawn_failed"
	}
	// Spawn errors quote argv[0], which may be most of a 1 MiB header and
	// grows under JSON escaping; the reply must stay a valid frame.
	message := err.Error()
	if len(message) > maxCapturedBytes {
		message = message[:maxCapturedBytes]
	}
	_ = c.send(errorFrame{"error", frame.ID, code, message}, nil)
}

func (a *agent) opSpec(frame inboundFrame) (opSpec, string, error) {
	spec := opSpec{env: a.baseEnv, cwd: "/"}
	if os.Geteuid() == 0 {
		spec.cred = &syscall.Credential{Uid: frame.UID, Gid: frame.GID, Groups: []uint32{}}
	} else if int(frame.UID) != os.Geteuid() || int(frame.GID) != os.Getegid() {
		return opSpec{}, "credentials", errors.New("the agent cannot change credentials")
	}
	if frame.Type == "exec" {
		if err := validateExec(frame); err != nil {
			return opSpec{}, "invalid", err
		}
		spec.argv, spec.cwd, spec.input = frame.Argv, frame.Cwd, frame.Stdin
		spec.env = mergeEnvironment(a.baseEnv, frame.Env)
		spec.forward = [2]bool{true, true}
		return spec, "", nil
	}
	if err := validateFilePath(frame.Path); err != nil {
		return opSpec{}, "invalid", err
	}
	helper := append([]string{}, a.helper...)
	if frame.Type == "stat" {
		spec.argv = append(helper, "stat", frame.Path)
		return spec, "", nil
	}
	if frame.MaxBytes < 1 || frame.MaxBytes > maxFileOperationBytes {
		return opSpec{}, "invalid", errors.New("max_bytes must be 1..268435456")
	}
	operation := "read"
	if frame.Type == "write_file" {
		operation, spec.input = "write", true
	} else {
		spec.forward[0] = true
	}
	spec.argv = append(helper, operation, frame.Path, strconv.FormatInt(frame.MaxBytes, 10))
	return spec, "", nil
}

func validateExec(frame inboundFrame) error {
	if len(frame.Argv) == 0 || len(frame.Argv) > 4096 {
		return errors.New("argv must hold 1..4096 items")
	}
	for _, item := range frame.Argv {
		if strings.IndexByte(item, 0) >= 0 {
			return errors.New("argv contains NUL")
		}
	}
	for key, value := range frame.Env {
		if !envKeyPattern.MatchString(key) || strings.IndexByte(value, 0) >= 0 {
			return errors.New("environment is invalid")
		}
	}
	if !filepath.IsAbs(frame.Cwd) || strings.IndexByte(frame.Cwd, 0) >= 0 {
		return errors.New("cwd must be absolute")
	}
	return nil
}

type outputStream struct {
	name     string
	file     *os.File // read end; never reassigned
	forward  bool
	sent     int64 // forwarded: bytes committed to the connection; captured: bytes read
	credit   int64
	captured []byte
	waiting  bool // blocked on the node (credit or a send), which is not idleness
	progress time.Time
}

type agentOp struct {
	conn       *agentConn
	id         uint64
	kind       string
	process    *os.Process
	input      *os.File // stdin write end; nil without input
	out        [2]*outputStream
	pumpsDone  chan struct{}
	takesInput bool

	mu        sync.Mutex
	cond      *sync.Cond
	cancelled bool
	// Set under mu before reaping: until then the pid, and so the process
	// group id, cannot be reused, which makes a group signal safe.
	reaped   bool
	finished bool
	pumping  int
	// received ≥ consumed ≥ credited.
	queue         [][]byte
	inputClosed   bool
	inputBroken   bool
	inputReceived int64
	inputConsumed int64
	inputCredited int64
}

func (c *agentConn) spawn(id uint64, kind string, spec opSpec) (*agentOp, error) {
	executable, err := resolveExecutable(spec.argv[0], spec.env)
	if err != nil {
		return nil, fmt.Errorf("resolve %q: %w", spec.argv[0], err)
	}
	op := &agentOp{conn: c, id: id, kind: kind, pumping: 2, pumpsDone: make(chan struct{}), takesInput: spec.input}
	op.cond = sync.NewCond(&op.mu)
	var child [3]*os.File
	defer func() {
		for _, file := range child {
			if file != nil {
				_ = file.Close()
			}
		}
	}()
	if spec.input {
		if child[0], op.input, err = os.Pipe(); err != nil {
			return nil, err
		}
	} else if child[0], err = os.Open(os.DevNull); err != nil {
		return nil, err
	}
	now := time.Now()
	for index := range op.out {
		reader, writer, err := os.Pipe()
		if err != nil {
			op.closeFiles()
			return nil, err
		}
		child[index+1] = writer
		op.out[index] = &outputStream{name: streamNames[index], file: reader, forward: spec.forward[index], progress: now}
	}
	op.process, err = os.StartProcess(executable, spec.argv, &os.ProcAttr{
		Dir:   spec.cwd,
		Env:   spec.env,
		Files: child[:],
		Sys:   &syscall.SysProcAttr{Setpgid: true, Credential: spec.cred},
	})
	if err != nil {
		op.closeFiles()
		return nil, err
	}
	return op, nil
}

func (op *agentOp) closeFiles() {
	if op.input != nil {
		_ = op.input.Close()
	}
	for _, stream := range op.out {
		if stream != nil {
			_ = stream.file.Close()
		}
	}
}

func (c *agentConn) run(op *agentOp) {
	c.mu.Lock()
	closed := c.closed.Load()
	if !closed {
		c.ops[op.id] = op
	}
	c.mu.Unlock()
	if !closed && op.kind == "exec" {
		_ = c.send(startedFrame{"started", op.id, op.process.Pid}, nil)
	}
	for _, stream := range op.out {
		go op.pump(stream)
	}
	if op.input != nil {
		go op.feed()
	}
	go op.wait()
	if closed {
		op.cancel()
	}
}

func (op *agentOp) pump(stream *outputStream) {
	size := maxCapturedBytes
	if stream.forward {
		size = maxOutputChunk
	}
	buffer := make([]byte, size)
	window := op.conn.window
	for {
		limit := len(buffer)
		if stream.forward {
			op.mu.Lock()
			for !op.cancelled && stream.sent-stream.credit >= window {
				stream.waiting = true
				op.cond.Wait()
			}
			// Time spent on the node, in a credit wait or a send, is not
			// idleness: the grace restarts when it ends.
			if stream.waiting {
				stream.waiting, stream.progress = false, time.Now()
			}
			cancelled := op.cancelled
			room := window - (stream.sent - stream.credit)
			op.mu.Unlock()
			if cancelled {
				break
			}
			limit = int(min(room, int64(limit)))
		}
		count, err := stream.file.Read(buffer[:limit])
		if count > 0 {
			op.mu.Lock()
			stream.progress = time.Now()
			// Count before sending: a credit may come back before send returns.
			stream.sent += int64(count)
			if !stream.forward && len(stream.captured) < maxCapturedBytes {
				stream.captured = append(stream.captured, buffer[:min(count, maxCapturedBytes-len(stream.captured))]...)
			}
			send := stream.forward && !op.cancelled
			stream.waiting = send
			op.mu.Unlock()
			if send {
				_ = op.conn.send(outputFrame{"output", op.id, stream.name}, buffer[:count])
			}
		}
		if err != nil {
			break
		}
	}
	op.mu.Lock()
	op.pumping--
	if op.pumping == 0 {
		close(op.pumpsDone)
	}
	op.mu.Unlock()
}

func (op *agentOp) credit(name string, offset int64) error {
	index := -1
	for candidate, streamName := range streamNames {
		if name == streamName {
			index = candidate
		}
	}
	if index < 0 || !op.out[index].forward {
		return violation("credit for %q, which op %d does not stream", name, op.id)
	}
	op.mu.Lock()
	defer op.mu.Unlock()
	stream := op.out[index]
	if offset < stream.credit || offset > stream.sent {
		return violation("credit %d is outside %d..%d", offset, stream.credit, stream.sent)
	}
	stream.credit = offset
	op.cond.Broadcast()
	return nil
}

func (op *agentOp) addInput(payload []byte) error {
	if !op.takesInput {
		return violation("op %d takes no input", op.id)
	}
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.inputClosed {
		return violation("input after input_close")
	}
	op.inputReceived += int64(len(payload))
	if op.inputReceived-op.inputCredited > op.conn.window {
		return violation("input exceeds its credit")
	}
	op.queue = append(op.queue, payload)
	op.cond.Broadcast()
	return nil
}

func (op *agentOp) closeInput() error {
	if !op.takesInput {
		return violation("op %d takes no input", op.id)
	}
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.inputClosed {
		return violation("second input_close")
	}
	op.inputClosed = true
	op.cond.Broadcast()
	return nil
}

// feed writes queued input to the child. After a write error (the child
// closed stdin) input is discarded but still credited, so the node never
// blocks on an op that no longer reads.
func (op *agentOp) feed() {
	defer op.input.Close()
	for {
		op.mu.Lock()
		for len(op.queue) == 0 && !op.inputClosed && !op.cancelled && !op.finished {
			op.cond.Wait()
		}
		if op.cancelled || op.finished || len(op.queue) == 0 {
			op.mu.Unlock()
			return
		}
		chunk := op.queue[0]
		op.queue[0] = nil
		op.queue = op.queue[1:]
		broken := op.inputBroken
		op.mu.Unlock()
		if !broken {
			_, err := op.input.Write(chunk)
			broken = err != nil
		}
		op.mu.Lock()
		op.inputBroken = broken
		op.inputConsumed += int64(len(chunk))
		credit := int64(-1)
		if op.inputConsumed-op.inputCredited >= op.conn.window/4 {
			op.inputCredited = op.inputConsumed
			credit = op.inputCredited
		}
		op.mu.Unlock()
		if credit >= 0 {
			_ = op.conn.send(inputCreditFrame{"input_credit", op.id, credit}, nil)
		}
	}
}

func (op *agentOp) signal(number syscall.Signal) {
	op.mu.Lock()
	defer op.mu.Unlock()
	if !op.reaped {
		_ = syscall.Kill(-op.process.Pid, number)
	}
}

// cancel kills the op for a lost connection. It reports whether the op was
// still live.
func (op *agentOp) cancel() bool {
	op.mu.Lock()
	if op.cancelled || op.finished {
		op.mu.Unlock()
		return false
	}
	op.cancelled = true
	if !op.reaped {
		_ = syscall.Kill(-op.process.Pid, syscall.SIGKILL)
	}
	op.cond.Broadcast()
	op.mu.Unlock()
	// Descendants outside the group may hold the pipes; never wait for them.
	op.closeFiles()
	return true
}

func (op *agentOp) wait() {
	_ = waitExited(op.process.Pid)
	op.mu.Lock()
	op.reaped = true
	now := time.Now()
	for _, stream := range op.out {
		if stream.progress.Before(now) {
			stream.progress = now
		}
	}
	op.mu.Unlock()
	state, waitErr := op.process.Wait()
	complete := op.awaitOutput()
	terminal := op.terminal(state, waitErr, complete)
	op.mu.Lock()
	cancelled := op.cancelled
	op.finished = true
	op.cond.Broadcast()
	op.mu.Unlock()
	// Unregister first: once the node has the terminal frame it may reuse
	// the op's slot under maxAgentOps.
	op.conn.mu.Lock()
	delete(op.conn.ops, op.id)
	op.conn.mu.Unlock()
	if !cancelled {
		_ = op.conn.send(terminal, nil)
	}
	for _, stream := range op.out {
		_ = stream.file.Close()
	}
}

// awaitOutput waits for both pumps. Once neither has read for the idle grace,
// except while waiting for credit, it closes the read ends and reports the
// output incomplete.
func (op *agentOp) awaitOutput() bool {
	select {
	case <-op.pumpsDone:
		return true
	default:
	}
	const poll = 100 * time.Millisecond
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	last := time.Now()
	for {
		select {
		case <-op.pumpsDone:
			return true
		case <-ticker.C:
		}
		now := time.Now()
		// A tick this late means the guest was frozen, which is not
		// idleness either: a pump may not have run since the thaw.
		frozen := now.Sub(last) > 2*poll
		last = now
		op.mu.Lock()
		idle := true
		for _, stream := range op.out {
			if frozen {
				stream.progress = now
			}
			if stream.waiting || now.Sub(stream.progress) < outputIdleGrace {
				idle = false
			}
		}
		op.mu.Unlock()
		if idle {
			for _, stream := range op.out {
				_ = stream.file.Close()
			}
			<-op.pumpsDone
			return false
		}
	}
}

func (op *agentOp) terminal(state *os.ProcessState, waitErr error, complete bool) any {
	if waitErr != nil {
		return errorFrame{"error", op.id, "failed", "wait: " + waitErr.Error()}
	}
	status, _ := state.Sys().(syscall.WaitStatus)
	exitCode, signalNumber := status.ExitStatus(), 0
	if status.Signaled() {
		signalNumber = int(status.Signal())
	}
	op.mu.Lock()
	stdoutBytes, stderrBytes := op.out[0].sent, op.out[1].sent
	stdout, stderr := op.out[0].captured, op.out[1].captured
	op.mu.Unlock()
	if op.kind == "exec" {
		frame := exitFrame{Type: "exit", ID: op.id, StdoutBytes: stdoutBytes, StderrBytes: stderrBytes, OutputComplete: complete}
		if signalNumber != 0 {
			frame.Signal = &signalNumber
		} else {
			frame.ExitCode = &exitCode
		}
		return frame
	}
	if signalNumber != 0 {
		return errorFrame{"error", op.id, "killed", "file helper killed by signal " + strconv.Itoa(signalNumber)}
	}
	if exitCode != 0 {
		code, known := fileErrorCodes[exitCode]
		if !known {
			code = "failed"
		}
		message := strings.TrimPrefix(strings.TrimSpace(string(stderr)), "file operation failed: ")
		return errorFrame{"error", op.id, code, message}
	}
	if op.kind != "stat" {
		return doneFrame{"done", op.id, nil}
	}
	stat := new(fileStat)
	decoder := json.NewDecoder(bytes.NewReader(stdout))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(stat); err != nil || len(stdout) >= maxCapturedBytes {
		return errorFrame{"error", op.id, "failed", "file helper returned an invalid stat"}
	}
	return doneFrame{"done", op.id, stat}
}

// waitExited blocks until pid exits without reaping it (waitid WNOWAIT), so
// the pid stays reserved until the caller reaps it.
func waitExited(pid int) error {
	var info [16]uint64 // siginfo_t
	for {
		_, _, errno := syscall.Syscall6(syscall.SYS_WAITID, 1 /* P_PID */, uintptr(pid),
			uintptr(unsafe.Pointer(&info)), syscall.WEXITED|0x1000000 /* WNOWAIT */, 0, 0)
		if errno != syscall.EINTR {
			if errno != 0 {
				return errno
			}
			return nil
		}
	}
}
