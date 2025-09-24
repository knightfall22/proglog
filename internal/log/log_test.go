package log

import (
	"bytes"
	"io"
	"os"
	"testing"

	proglog "github.com/knightfall22/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, log *Log,
	){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "segment-tests")
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				err := os.RemoveAll(dir)
				if err != nil {
					t.Fatal(err)
				}
			}()
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			if err != nil {
				t.Fatal("creating new log failed")
			}
			fn(t, log)

		})
	}
}

func testAppendRead(t *testing.T, l *Log) {
	value := &proglog.Record{
		Value: []byte("Hello, Angel"),
	}

	off, err := l.Append(value)
	if err != nil {
		t.Fatalf("error appending to log %v", err)
	}

	if off != 0 {
		t.Fatalf("offset mismatch expected %d got %d", 0, off)
	}

	res, err := l.Read(off)
	if err != nil {
		t.Fatalf("error reading from log %v", err)
	}

	if !bytes.Equal(res.Value, res.Value) {
		t.Fatal("value mismatch")
	}

	l.Close()
}

func testOutOfRangeErr(t *testing.T, l *Log) {
	_, err := l.Read(1)
	if err == nil {
		t.Fatal("expected error")
	}

	apiErr := err.(proglog.ErrOffsetOutOfRange)

	if apiErr.Offset != 1 {
		t.Fatal("offset mismatch")
	}

	l.Close()
}

func testInitExisting(t *testing.T, o *Log) {
	value := &proglog.Record{
		Value: []byte("Hello, Angel"),
	}

	for range 3 {
		_, err := o.Append(value)
		if err != nil {
			t.Fatalf("error appending to log %v", err)
		}
	}

	if err := o.Close(); err != nil {
		t.Fatalf("error closing file %v", err)
	}

	off, err := o.LowestOffset()
	if err != nil {
		t.Fatalf("error retrieving lowest offset %v", err)
	}

	if off != 0 {
		t.Fatalf("offset mismatch expected %d got %d", 0, off)
	}

	off, err = o.HighestOffset()
	if err != nil {
		t.Fatalf("error retrieving lowest offset %v", err)
	}

	if off != 2 {
		t.Fatalf("offset mismatch expected %d got %d", 2, off)
	}

	n, err := NewLog(o.Dir, o.Config)
	if err != nil {
		t.Fatal("creating new log failed")
	}

	off, err = n.LowestOffset()
	if err != nil {
		t.Fatalf("error retrieving lowest offset %v", err)
	}

	if off != 0 {
		t.Fatalf("offset mismatch expected %d got %d", 0, off)
	}

	off, err = n.HighestOffset()
	if err != nil {
		t.Fatalf("error retrieving lowest offset %v", err)
	}

	if off != 2 {
		t.Fatalf("offset mismatch expected %d got %d", 2, off)
	}

	n.Close()
}

func testReader(t *testing.T, log *Log) {
	value := &proglog.Record{
		Value: []byte("Hello, Angel"),
	}

	off, err := log.Append(value)
	if err != nil {
		t.Fatalf("error appending to log %v", err)
	}

	if off != 0 {
		t.Fatalf("offset mismatch expected %d got %d", 0, off)
	}

	reader := log.Reader()
	b, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("error reading from reader %v", err)
	}
	read := &proglog.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	if err != nil {
		t.Fatalf("error unmarshalling %v", err)
	}

	if !bytes.Equal(read.Value, value.Value) {
		t.Fatal("value mismatch")
	}

	log.Close()
}

func testTruncate(t *testing.T, log *Log) {
	value := &proglog.Record{
		Value: []byte("hello world"),
	}

	for range 3 {
		_, err := log.Append(value)
		if err != nil {
			t.Fatalf("error appending to log %v", err)
		}
	}

	if err := log.Truncate(1); err != nil {
		t.Fatalf("error truncating log %v", err)
	}

	if _, err := log.Read(1); err == nil {
		t.Fatal("expected error")
	}
	log.Close()

}
