package main

import (
	"archive/tar"
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

type archiveMember struct {
	name     string
	typeflag byte
	mode     int64
	body     string
}

func tarStream(t *testing.T, members ...archiveMember) *bytes.Buffer {
	t.Helper()
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	for _, member := range members {
		header := &tar.Header{Name: member.name, Typeflag: member.typeflag, Mode: member.mode, Size: int64(len(member.body))}
		if member.typeflag != tar.TypeReg {
			header.Size, header.Linkname = 0, "target"
		}
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write([]byte(member.body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return &buffer
}

func TestFilesExtractWritesFilesModesAndEmptyDirectories(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "work")
	old := filepath.Join(root, "old")
	if err := os.WriteFile(old, []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(destination, "lib"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(old, filepath.Join(destination, "run.sh")); err != nil {
		t.Fatal(err)
	}
	input := tarStream(t,
		archiveMember{"run.sh", tar.TypeReg, 0o4755, "#!/bin/sh\n"},
		archiveMember{"lib/a.py", tar.TypeReg, 0o644, "a"},
		archiveMember{"lib/deep/b.py", tar.TypeReg, 0o600, ""},
	)
	if err := runFiles([]string{"extract", destination, "11", "empty/leaf"}, input, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]os.FileMode{"run.sh": 0o755, "lib/a.py": 0o644, "lib/deep/b.py": 0o600} {
		info, err := os.Lstat(filepath.Join(destination, name))
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != want {
			t.Fatalf("%s: %v %v", name, info, err)
		}
	}
	if data, _ := os.ReadFile(old); string(data) != "untouched" {
		t.Fatal("followed destination symlink")
	}
	if info, err := os.Stat(filepath.Join(destination, "lib")); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatal("changed an existing directory's mode")
	}
	if info, err := os.Stat(filepath.Join(destination, "empty", "leaf")); err != nil || !info.IsDir() {
		t.Fatal("empty directory missing")
	}
	entries, _ := os.ReadDir(filepath.Join(destination, "lib"))
	if len(entries) != 2 {
		t.Fatal("temporary files leaked")
	}
}

func TestFilesExtractRefusesUnsafeMembersAndLimits(t *testing.T) {
	cases := map[string]struct {
		input       *bytes.Buffer
		limit       string
		directories []string
		exit        int
	}{
		"symlink":     {tarStream(t, archiveMember{"link", tar.TypeSymlink, 0o777, ""}), "8", nil, fileExitInvalid},
		"hardlink":    {tarStream(t, archiveMember{"link", tar.TypeLink, 0o644, ""}), "8", nil, fileExitInvalid},
		"directory":   {tarStream(t, archiveMember{"dir/", tar.TypeDir, 0o755, ""}), "8", nil, fileExitInvalid},
		"parent":      {tarStream(t, archiveMember{"../escape", tar.TypeReg, 0o644, "x"}), "8", nil, fileExitInvalid},
		"absolute":    {tarStream(t, archiveMember{"/escape", tar.TypeReg, 0o644, "x"}), "8", nil, fileExitInvalid},
		"dot":         {tarStream(t, archiveMember{"./a", tar.TypeReg, 0o644, "x"}), "8", nil, fileExitInvalid},
		"too large":   {tarStream(t, archiveMember{"a", tar.TypeReg, 0o644, "abc"}, archiveMember{"b", tar.TypeReg, 0o644, "abc"}), "5", nil, fileExitTooLarge},
		"bad dir arg": {tarStream(t), "8", []string{"../up"}, fileExitInvalid},
		"not a tar":   {bytes.NewBufferString(strings.Repeat("x", 1024)), "8", nil, fileExitInvalid},
	}
	for name, test := range cases {
		root := t.TempDir()
		args := append([]string{"extract", filepath.Join(root, "work"), test.limit}, test.directories...)
		err := runFiles(args, test.input, &bytes.Buffer{})
		if err == nil || fileExitCode(err) != test.exit {
			t.Fatalf("%s: %v", name, err)
		}
		if _, err := os.Lstat(filepath.Join(root, "escape")); err == nil {
			t.Fatalf("%s: wrote outside the destination", name)
		}
	}
	if err := runFiles([]string{"extract", "relative", "8"}, tarStream(t), &bytes.Buffer{}); err == nil {
		t.Fatal("accepted a relative destination")
	}
}
