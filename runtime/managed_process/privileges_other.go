//go:build !linux

package main

import (
	"errors"
	"os"
)

func dropWorkloadPrivileges(uid, gid uint32) error {
	if int(uid) != os.Geteuid() || int(gid) != os.Getegid() {
		return errors.New("credential changes require Linux")
	}
	return nil
}
