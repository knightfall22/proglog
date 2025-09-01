package server

import (
	"fmt"
	"sync"
)

var ErrOffsetNotFound = fmt.Errorf("offset not found")

type Record struct {
	Value  []byte `json:"value"`
	Offset uint   `json:"offset"`
}

type Log struct {
	records []Record
	mu      sync.Mutex
}

func NewLog() *Log {
	return &Log{}
}

func (c *Log) Append(r Record) (uint, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	r.Offset = uint(len(c.records))
	c.records = append(c.records, r)

	return r.Offset, nil
}

func (c *Log) Read(offset uint) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if offset >= uint(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}

	return c.records[offset], nil
}
