package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := index{file: f}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())

	// We do this because mmap locks the file preventing it from growing as new data are added
	if err := os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return &idx, nil
}

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := i.mmap.UnsafeUnmap(); err != nil {
		return err
	}

	if err := i.file.Sync(); err != nil {
		return err
	}

	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Read(in int64) (off uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		off = uint32((i.size / entWidth) - 1)
	} else {
		off = uint32(in)
	}

	pos = uint64(off) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	off = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return
}

func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}

// When we start our service, the service needs to know the offset to set on the
// next record appended to the log. The service learns the next record’s offset
// by looking at the last entry of the index, a simple process of reading the last
// 12 bytes of the file. However, we mess up this process when we grow the files
// so we can memory-map them. (The reason we resize them now is that, once
// they’re memory-mapped, we can’t resize them, so it’s now or never.) We grow
// the files by appending empty space at the end of them, so the last entry is no
// longer at the end of the file—instead, there’s some unknown amount of space
// between this entry and the file’s end. This space prevents the service from
// restarting properly. That’s why we shut down the service by truncating the
// index files to remove the empty space and put the last entry at the end of the
// file once again. This graceful shutdown returns the service to a state where
// it can restart properly and efficiently.
