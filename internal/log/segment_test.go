package log

import (
	"bytes"
	"io"
	"os"
	"testing"

	proglog "github.com/knightfall22/proglog/api/v1"
)

func TestSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	want := &proglog.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	s, err := newSegment(dir, 16, c)
	if err != nil {
		t.Fatalf("error creating segment %v", err)
	}

	if s.IsMaxed() {
		t.Fatal("segment should not be maxxed out")
	}

	if s.baseOffset != 16 || s.nextOffset != 16 {
		t.Fatal("baseOffset or nextOffset do not match")
	}

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		if err != nil {
			t.Fatalf("error occured while appending %v", err)
		}

		if off != 16+i {
			t.Fatalf("expected %d got %d", 16+1, off)
		}

		got, err := s.Read(off)
		if err != nil {
			t.Fatalf("error occured while reading %v", err)
		}

		if !bytes.Equal(got.Value, want.Value) {
			t.Fatalf("expected %s got %s", want.Value, got.Value)
		}
	}

	_, err = s.Append(want)
	if err != io.EOF {
		t.Fatalf("error not EOF %v", err)
	}

	if !s.IsMaxed() {
		t.Fatal("segment should be maxxed out")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("error closing segment %v", err)
	}

	s, err = newSegment(dir, 16, c)
	if err != nil {
		t.Fatalf("error creating segment %v", err)
	}

	if !s.IsMaxed() {
		t.Fatal("segment should be maxxed out")
	}

	if err := s.Remove(); err != nil {
		t.Fatalf("error occured while removing %v", err)
	}

	s, err = newSegment(dir, 16, c)
	if err != nil {
		t.Fatalf("error creating segment %v", err)
	}

	if s.IsMaxed() {
		t.Fatal("segment should not be maxxed out")
	}
}
