package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

func FuzzControlRequestDecoding(f *testing.F) {
	f.Add([]byte(`{"version":1,"action":"status","job_id":"job-1"}`))
	f.Add([]byte(`{"version":1,"action":"status","unknown":true}`))
	f.Add([]byte(`not-json`))
	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > maxRequestBytes+1 {
			t.Skip()
		}
		req, err := decodeControlRequest(bytes.NewReader(payload))
		if err != nil {
			return
		}
		encoded, err := json.Marshal(req)
		if err != nil {
			t.Fatalf("decoded request cannot be encoded: %v", err)
		}
		var roundTrip request
		if err := json.Unmarshal(encoded, &roundTrip); err != nil {
			t.Fatalf("encoded request cannot be decoded: %v", err)
		}
	})
}

func FuzzValidatedSpec(f *testing.F) {
	f.Add([]byte(`{"job_id":"job-1","argv":["true"],"env":{},"cwd":"/","max_stdout_bytes":1,"max_stderr_bytes":1}`))
	f.Add([]byte(`{"job_id":"bad/id","argv":[],"cwd":"relative"}`))
	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > maxRequestBytes {
			t.Skip()
		}
		var req request
		if err := json.Unmarshal(payload, &req); err != nil {
			return
		}
		spec, digest, err := validatedSpec(req)
		if err != nil {
			return
		}
		decodedDigest, decodeErr := hex.DecodeString(digest)
		if decodeErr != nil || len(decodedDigest) != sha256.Size {
			t.Fatalf("validated digest is malformed: %q", digest)
		}
		encoded, err := json.Marshal(spec)
		if err != nil {
			t.Fatalf("validated spec cannot be encoded: %v", err)
		}
		expected := sha256.Sum256(encoded)
		if digest != hex.EncodeToString(expected[:]) {
			t.Fatalf("validated digest does not bind the canonical spec")
		}
	})
}

func FuzzResponseEncoding(f *testing.F) {
	f.Add("ordinary failure", []byte("log output"))
	f.Add("", []byte{})
	f.Add("\x9c", []byte{})
	f.Fuzz(func(t *testing.T, message string, data []byte) {
		if len(message) > 4096 || len(data) > maxLogReadBytes {
			t.Skip()
		}
		original := response{
			Version: protocolVersion,
			OK:      message == "",
			Error:   message,
			Stream:  "stdout",
			Next:    int64(len(data)),
			EOF:     true,
			Data:    data,
		}
		var encoded bytes.Buffer
		writeResponse(&encoded, original)
		if encoded.Len() > maxResponseBytes {
			t.Fatalf("response exceeded protocol bound: %d", encoded.Len())
		}
		var decoded response
		if err := json.Unmarshal(encoded.Bytes(), &decoded); err != nil {
			t.Fatalf("response is invalid JSON: %v", err)
		}
		normalizedMessageBytes, err := json.Marshal(message)
		if err != nil {
			t.Fatalf("message cannot be JSON encoded: %v", err)
		}
		var normalizedMessage string
		if err := json.Unmarshal(normalizedMessageBytes, &normalizedMessage); err != nil {
			t.Fatalf("message cannot be JSON decoded: %v", err)
		}
		if decoded.Version != original.Version || decoded.Error != normalizedMessage || !bytes.Equal(decoded.Data, data) {
			t.Fatalf("response changed during encoding")
		}
	})
}
