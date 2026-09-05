package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFilesAtomicReplacementAndLimits(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "nested", "space ü :,$")
	if err := runFiles([]string{"write", target, "8"}, strings.NewReader("original"), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if err := runFiles([]string{"write", target, "2"}, strings.NewReader("too big"), &bytes.Buffer{}); err == nil {
		t.Fatal("oversize write succeeded")
	}
	data, err := os.ReadFile(target)
	if err != nil || string(data) != "original" {
		t.Fatalf("old file lost: %q %v", data, err)
	}
	info, _ := os.Stat(target)
	if info.Mode().Perm() != 0600 {
		t.Fatal(info.Mode())
	}
	var out bytes.Buffer
	if err := runFiles([]string{"read", target, "8"}, nil, &out); err != nil || out.String() != "original" {
		t.Fatalf("read failed: %v", err)
	}
	out.Reset()
	if err := runFiles([]string{"read", target, "2"}, nil, &out); err == nil || out.Len() != 0 {
		t.Fatal("oversize read succeeded")
	}
	entries, _ := os.ReadDir(filepath.Dir(target))
	if len(entries) != 1 {
		t.Fatal("temporary files leaked")
	}
}

func TestFilesReplaceSymlinkAndRejectDirectory(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	link := filepath.Join(root, "link")
	if err := os.WriteFile(target, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := runFiles([]string{"write", link, "8"}, strings.NewReader("new"), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(target)
	if string(data) != "original" {
		t.Fatal("followed destination symlink")
	}
	for _, path := range []string{root, root + "/", root + "/../escape", "relative"} {
		if err := runFiles([]string{"write", path, "8"}, strings.NewReader("new"), &bytes.Buffer{}); err == nil {
			t.Fatalf("accepted %q", path)
		}
	}
	if err := runFiles([]string{"read", root, "8"}, nil, &bytes.Buffer{}); err == nil {
		t.Fatal("read directory")
	}
}
