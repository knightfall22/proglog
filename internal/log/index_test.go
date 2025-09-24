package log

import (
	"io"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {
	file, err := os.CreateTemp("", "log_file")
	if err != nil {
		t.Fatal(err)
	}

	c := Config{}
	c.Segment.MaxIndexBytes = 1024

	idx, err := newIndex(file, c)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		idx.Close()
		os.Remove(file.Name())
	}()

	_, _, err = idx.Read(-1)
	if err == nil {
		t.Fatal("expected error while reading an empty index")
	}

	if idx.Name() != file.Name() {
		t.Fatalf("name mismatch expected %s got %s", file.Name(), idx.Name())
	}

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, want := range entries {
		if err := idx.Write(want.Off, want.Pos); err != nil {
			t.Fatalf("write failed %v", err)
		}

		_, pos, err := idx.Read(int64(want.Off))
		if err != nil {
			t.Fatalf("read failed %v", err)
		}

		if want.Pos != pos {
			t.Fatalf("postion value mismatch expected %d got %d", want.Pos, pos)
		}
	}

	// index and scanner should error when reading past existing entries
	_, _, err = idx.Read(int64(len(entries)))
	if err != io.EOF {
		t.Fatalf("error not EOF %v", err)
	}

	err = idx.Close()
	if err != nil {
		t.Fatalf("error occured while closing %v", err)
	}

	file, err = os.OpenFile(file.Name(), os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("cannot open file %v", err)
	}

	idx, err = newIndex(file, c)
	if err != nil {
		t.Fatal(err)
	}

	_, pos, err := idx.Read(-1)
	if err != nil {
		t.Fatalf("read failed %v", err)
	}

	if pos != entries[1].Pos {
		t.Fatalf("postion value mismatch after close expected %d got %d", entries[1].Pos, pos)
	}

}
