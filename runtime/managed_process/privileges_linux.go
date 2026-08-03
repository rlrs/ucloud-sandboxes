//go:build linux

package main

import (
	"fmt"
	"syscall"
	"unsafe"
)

type capabilityHeader struct {
	Version uint32
	PID     int32
}

type capabilityData struct {
	Effective   uint32
	Permitted   uint32
	Inheritable uint32
}

const linuxCapabilityVersion3 = 0x20080522

func dropWorkloadPrivileges(uid, gid uint32) error {
	if err := syscall.Setgroups([]int{}); err != nil && syscall.Geteuid() == 0 {
		return fmt.Errorf("clear supplementary groups: %w", err)
	}
	if err := syscall.Setgid(int(gid)); err != nil {
		return fmt.Errorf("set gid: %w", err)
	}
	if err := syscall.Setuid(int(uid)); err != nil {
		return fmt.Errorf("set uid: %w", err)
	}
	header := capabilityHeader{Version: linuxCapabilityVersion3}
	data := [2]capabilityData{}
	_, _, errno := syscall.RawSyscall(
		syscall.SYS_CAPSET,
		uintptr(unsafe.Pointer(&header)),
		uintptr(unsafe.Pointer(&data[0])),
		0,
	)
	if errno != 0 {
		return fmt.Errorf("clear capabilities: %w", errno)
	}
	return nil
}
