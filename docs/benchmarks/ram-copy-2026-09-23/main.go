package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func CopySparseApplicationMemory(dst, src *os.File) error {
	var source, target syscall.Stat_t
	if err := syscall.Fstat(int(src.Fd()), &source); err != nil {
		return err
	}
	if err := syscall.Fstat(int(dst.Fd()), &target); err != nil {
		return err
	}
	if source.Mode&syscall.S_IFMT != syscall.S_IFREG || target.Mode&syscall.S_IFMT != syscall.S_IFREG || target.Size != 0 || (source.Dev == target.Dev && source.Ino == target.Ino) {
		return fmt.Errorf("sparse application memory requires a regular source and distinct empty target")
	}
	if err := dst.Truncate(source.Size); err != nil {
		return err
	}
	buffer := make([]byte, bufferBytes)
	for cursor := int64(0); cursor < source.Size; {
		data, err := syscall.Seek(int(src.Fd()), cursor, 3)
		if err == syscall.ENXIO {
			break
		}
		if err != nil {
			return err
		}
		hole, err := syscall.Seek(int(src.Fd()), data, 4)
		if err != nil {
			return err
		}
		if data < cursor || hole <= data || hole > source.Size {
			return fmt.Errorf("invalid application memory extent")
		}
		for offset := data; offset < hole; {
			size := int64(len(buffer))
			if hole-offset < size {
				size = hole - offset
			}
			n, err := src.ReadAt(buffer[:size], offset)
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrNoProgress
			}
			written, err := dst.WriteAt(buffer[:n], offset)
			if err != nil {
				return err
			}
			if written != n {
				return io.ErrShortWrite
			}
			offset += int64(n)
		}
		cursor = hole
	}
	var final syscall.Stat_t
	if err := syscall.Fstat(int(src.Fd()), &final); err != nil {
		return err
	}
	if final.Dev != source.Dev || final.Ino != source.Ino || final.Size != source.Size || final.Mtim != source.Mtim || final.Ctim != source.Ctim {
		return fmt.Errorf("application memory changed during sparse copy")
	}
	return nil
}

var bufferBytes = 256 * 1024

func statmap(path string) map[string]int64 {
	b, _ := os.ReadFile(path)
	m := map[string]int64{}
	for _, l := range strings.Split(string(b), "\n") {
		f := strings.Fields(l)
		if len(f) == 2 {
			v, _ := strconv.ParseInt(f[1], 10, 64)
			m[f[0]] = v
		}
	}
	return m
}
func kernelCopy(dst, src *os.File, mode string) error {
	var s syscall.Stat_t
	if e := syscall.Fstat(int(src.Fd()), &s); e != nil {
		return e
	}
	if e := dst.Truncate(s.Size); e != nil {
		return e
	}
	for cursor := int64(0); cursor < s.Size; {
		data, e := syscall.Seek(int(src.Fd()), cursor, 3)
		if e == syscall.ENXIO {
			break
		}
		if e != nil {
			return e
		}
		hole, e := syscall.Seek(int(src.Fd()), data, 4)
		if e != nil {
			return e
		}
		if _, e = dst.Seek(data, 0); e != nil {
			return e
		}
		offset := data
		for offset < hole {
			count := int(hole - offset)
			if count > 1024*1024 {
				count = 1024 * 1024
			}
			var n int
			if mode == "sendfile" {
				n, e = syscall.Sendfile(int(dst.Fd()), int(src.Fd()), &offset, count)
			} else {
				return fmt.Errorf("unknown")
			}
			if e != nil {
				return e
			}
			if n == 0 {
				return io.ErrNoProgress
			}
		}
		cursor = hole
	}
	return nil
}
func main() {
	src, e := os.Open(os.Args[1])
	if e != nil {
		panic(e)
	}
	defer src.Close()
	dst, e := os.OpenFile(os.Args[2], os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if e != nil {
		panic(e)
	}
	defer func() { dst.Close(); os.Remove(os.Args[2]) }()
	mode := os.Args[3]
	cg, _ := os.ReadFile("/proc/self/cgroup")
	root := "/sys/fs/cgroup/" + strings.TrimSpace(strings.TrimPrefix(string(cg), "0::"))
	before := statmap(root + "/cpu.stat")
	max, _ := os.ReadFile(root + "/cpu.max")
	mem0 := statmap(root + "/memory.stat")
	var r0, r1 syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &r0)
	started := time.Now()
	if strings.HasPrefix(mode, "buffer") {
		if mode == "buffer1m" {
			bufferBytes = 1024 * 1024
		}
		e = CopySparseApplicationMemory(dst, src)
	} else {
		e = kernelCopy(dst, src, mode)
	}
	elapsed := time.Since(started).Seconds()
	syscall.Getrusage(syscall.RUSAGE_SELF, &r1)
	after := statmap(root + "/cpu.stat")
	mem1 := statmap(root + "/memory.stat")
	out := map[string]interface{}{"mode": mode, "seconds": elapsed, "cpu_max": strings.TrimSpace(string(max)), "cpu_usec": after["usage_usec"] - before["usage_usec"], "throttled_usec": after["throttled_usec"] - before["throttled_usec"], "minor_faults": r1.Minflt - r0.Minflt, "major_faults": r1.Majflt - r0.Majflt, "shmem_delta": mem1["shmem"] - mem0["shmem"]}
	if e != nil {
		out["error"] = e.Error()
	} else {
		src.Seek(0, 0)
		dst.Seek(0, 0)
		a := sha256.New()
		b := sha256.New()
		io.Copy(a, src)
		io.Copy(b, dst)
		out["integrity"] = fmt.Sprintf("%x", a.Sum(nil)) == fmt.Sprintf("%x", b.Sum(nil))
		var s, t syscall.Stat_t
		syscall.Fstat(int(src.Fd()), &s)
		syscall.Fstat(int(dst.Fd()), &t)
		out["source_blocks"] = s.Blocks
		out["target_blocks"] = t.Blocks
		out["size"] = s.Size
	}
	json.NewEncoder(os.Stdout).Encode(out)
}
