package log

import (
	"bytes"
	"os"
	"testing"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func testAppend(t *testing.T, store *store) {
	t.Helper()

	for i := uint64(1); i < 4; i++ {
		n, pos, err := store.Append(write)
		if err != nil {
			t.Fatalf("Appending %v to log failed %v", write, err)
		}

		if pos+n != width*i {
			t.Fatalf("invalid width expected %d got %d", width*i, pos+n)
		}
	}
}
func testRead(t *testing.T, store *store) {
	t.Helper()

	var pos uint64
	for i := uint64(1); i < 4; i++ {
		buf, err := store.Read(pos)
		if err != nil {
			t.Fatalf("Appending %v to log failed %v", write, err)
		}

		if !bytes.Equal(buf, write) {
			t.Fatalf("expected %s got %s", write, buf)
		}

		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()

	for i, offset := uint64(1), int64(0); i < 4; i++ {
		buf := make([]byte, lenWidth)
		n, err := s.ReadAt(buf, offset)
		if err != nil {
			t.Fatalf("Reading at %d failed %v", offset, err)
		}

		offset += int64(n)
		size := enc.Uint64(buf)
		buf = make([]byte, size)

		n, err = s.ReadAt(buf, offset)
		if err != nil {
			t.Fatalf("Reading at %d failed %v", offset, err)
		}

		if n != int(size) {
			t.Fatalf("sizes don't match, expected %d got %d", size, n)
		}

		if !bytes.Equal(buf, write) {
			t.Fatalf("expected %s got %s", write, buf)
		}

		offset += int64(n)
	}
}

func TestAppendAndRead(t *testing.T) {
	file, err := os.CreateTemp("", "store_file")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())
	store, err := newStore(file)
	if err != nil {
		t.Fatal(err)
	}

	testAppend(t, store)
	testRead(t, store)
	testReadAt(t, store)

	store, err = newStore(file)
	if err != nil {
		t.Fatal(err)
	}
	testRead(t, store)
}

func TestStoreClose(t *testing.T) {
	file, err := os.CreateTemp("", "store_file")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())
	store, err := newStore(file)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = store.Append(write)
	if err != nil {
		t.Fatalf("Appending %v to log failed %v", write, err)
	}

	f, beforeSize, err := openFile(file.Name())
	if err != nil {
		t.Fatalf("error opening file %v", err)
	}

	if err = store.Close(); err != nil {
		t.Fatalf("error closing store %v", err)
	}

	f, afterSize, err := openFile(f.Name())
	if err != nil {
		t.Fatalf("error opening file %v", err)
	}

	if afterSize < beforeSize {
		t.Fatalf("expected file size mismatch")
	}

}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
