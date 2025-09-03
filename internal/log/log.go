package log

import (
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	proglog "github.com/knightfall22/proglog/api/v1"
)

type Log struct {
	mu            sync.RWMutex
	Config        Config
	Dir           string
	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, config Config) (*Log, error) {
	if config.Segment.MaxStoreBytes == 0 {
		config.Segment.MaxStoreBytes = 1024
	}

	if config.Segment.MaxIndexBytes == 0 {
		config.Segment.MaxIndexBytes = 1024
	}

	l := &Log{
		Dir:    dir,
		Config: config,
	}

	return l, l.setup()
}

func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++ {
		if err := l.newSegment(baseOffsets[i]); err != nil {
			return err
		}

		i++
	}

	if l.segments == nil {
		if err = l.newSegment(
			l.Config.Segment.InitialOffset,
		); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) newSegment(baseOffset uint64) error {
	segment, err := newSegment(l.Dir, baseOffset, l.Config)
	if err != nil {
		return err
	}

	l.segments = append(l.segments, segment)
	l.activeSegment = segment
	return nil
}

func (l *Log) Append(record *proglog.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMaxed() {
		if err := l.newSegment(off + 1); err != nil {
			return 0, err
		}
	}

	return off, nil
}

func (l *Log) Read(off uint64) (*proglog.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var s *segment

	sidx := slices.IndexFunc(l.segments, func(seg *segment) bool {
		return seg.baseOffset <= off && seg.nextOffset > off
	})

	if sidx == -1 {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}

	s = l.segments[sidx]

	if s == nil || s.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}
	return s.Read(off)
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment

	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}

	l.segments = segments
	return nil
}

func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))

	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}

	return io.MultiReader(readers...)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)

	o.off += int64(n)
	return n, err
}
