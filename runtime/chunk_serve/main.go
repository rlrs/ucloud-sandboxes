// Command ucloud-chunk-serve answers the chunk store node's reads from the
// extents on its disk, on every core. Anything else (a miss, a bad token, a
// malformed request, warm jobs, residency checks, health) goes to
// ucloud-chunk-store on the loopback address, so the Python node stays the one
// authority on fills, errors and control (docs/chunk-store-m2-plan.md §4.1).
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"
)

// exitNotConfigured matches ucloud-chunk-store: systemd keeps the unit stopped.
const exitNotConfigured = 78

type storeConfig struct {
	ReadTokenFile  string          `json:"read_token_file"`
	WriteTokenFile string          `json:"write_token_file"`
	Nydusd         json.RawMessage `json:"nydusd"`
	StoreNode      *struct {
		Listen      string `json:"listen"`
		CacheDir    string `json:"cache_dir"`
		ExtentBytes int64  `json:"extent_bytes"`
	} `json:"store_node"`
}

func main() {
	configPath := flag.String("config", "", "the chunk_store block (JSON), as store init writes it")
	listen := flag.String("listen", "", "address to serve (default: store_node.listen)")
	upstream := flag.String("upstream", "", "ucloud-chunk-store (default: 127.0.0.1:<listen port>)")
	flag.Parse()
	raw, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	var config storeConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		log.Fatalf("chunk store config: %v", err)
	}
	if config.StoreNode == nil {
		fmt.Println("immutable_environments.chunk_store.store_node is not configured")
		os.Exit(exitNotConfigured)
	}
	node := config.StoreNode
	if *listen == "" {
		*listen = node.Listen
	}
	if *upstream == "" {
		_, port, err := net.SplitHostPort(*listen)
		if err != nil {
			log.Fatalf("store_node.listen: %v", err)
		}
		*upstream = net.JoinHostPort("127.0.0.1", port)
	}
	if node.ExtentBytes < 1<<20 || node.ExtentBytes > 64<<20 || node.ExtentBytes&(node.ExtentBytes-1) != 0 {
		log.Fatal("store_node.extent_bytes must be a power of two in [1, 64] MiB")
	}
	tokens := make([][]byte, 2)
	for i, path := range []string{config.ReadTokenFile, config.WriteTokenFile} {
		if tokens[i], err = readToken(path); err != nil {
			log.Fatal(err)
		}
	}
	if string(tokens[0]) == string(tokens[1]) {
		log.Fatal("the chunk store needs distinct read and write tokens")
	}
	store, err := openExtents(node.CacheDir, node.ExtentBytes)
	if err != nil {
		log.Fatal(err)
	}
	virtual := len(config.Nydusd) > 0 && string(config.Nydusd) != "null"
	server, err := newServer(store, tokens[0], tokens[1], "http://"+*upstream, virtual)
	if err != nil {
		log.Fatal(err)
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("serving %d extents from %s on %s; everything else via %s", store.count(), node.CacheDir, *listen,
		*upstream)
	httpServer := &http.Server{Handler: server, ReadHeaderTimeout: 30 * time.Second, IdleTimeout: 5 * time.Minute,
		MaxHeaderBytes: 64 << 10, ErrorLog: log.New(os.Stderr, "", log.LstdFlags)}
	log.Fatal(httpServer.Serve(listener))
}

// readToken reads a bearer token as ucloud-chunk-store does: an owner-only
// regular file of the service user (or root), stripped, at most 4096 bytes.
func readToken(path string) ([]byte, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	owner := info.Sys().(*syscall.Stat_t).Uid
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || (owner != 0 && int(owner) != os.Geteuid()) {
		return nil, errors.New("chunk index token files must be owner-only regular files")
	}
	raw, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil {
		return nil, err
	}
	token := strings.TrimSpace(string(raw))
	if len(token) < 32 || len(token) > 4096 {
		return nil, errors.New("chunk index token has an invalid length")
	}
	return []byte(token), nil
}
