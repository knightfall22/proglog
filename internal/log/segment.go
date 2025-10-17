package log

import (
	"fmt"
	"os"
	"path"

	proglog "github.com/knightfall22/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

// A segment is a wrapper around index and store.
// A segment represents a portion of the dataset. Segments help with disk management by breaking down
// large amount of data into sizable chunks. Once a segment exceeds it's defined size a new segment is created.
type segment struct {
	index *index
	store *store
	// Baseoffset is used for segmention. It is useful for finding an offset in relation to itself.
	// View log.Read and log.Append for better understanding
	baseOffset uint64
	nextOffset uint64
	config     Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	storeFilePath := fmt.Sprintf("%d%s", baseOffset, ".store")
	storeFile, err := os.OpenFile(
		path.Join(dir, storeFilePath),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	indexFilePath := fmt.Sprintf("%d%s", baseOffset, ".index")
	indexFile, err := os.OpenFile(
		path.Join(dir, indexFilePath),
		os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	// If the index is empty, then the next record appended to the segment would be the first
	// record and its offset would be the segment’s base offset.
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		// 	If the index has at least one entry,
		// then that means the offset of the next record written should
		// take the offset at the end of the segment, which we get by adding 1 to the
		// base offset and relative offset.
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

func (s *segment) Append(record *proglog.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	if err = s.index.Write(
		// index offsets are relative to base offset
		uint32(s.nextOffset-s.baseOffset),
		pos,
	); err != nil {
		return 0, err
	}

	s.nextOffset++
	return cur, nil
}

func (s *segment) Read(off uint64) (*proglog.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	record := proglog.Record{}
	if err := proto.Unmarshal(p, &record); err != nil {
		return nil, err
	}

	return &record, nil
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes ||
		s.index.size+entWidth >= s.config.Segment.MaxIndexBytes
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// func nearestMultiple(j, k uint64) uint64 {
// 	if j >= 0 {
// 		return (j / k) * k
// 	}
// 	return ((j - k + 1) / k) * k
// }
