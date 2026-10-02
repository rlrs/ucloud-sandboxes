//go:build !linux

package main

import "errors"

func runAgent([]string) error {
	return errors.New("the guest agent requires Linux")
}
