package log

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	proglog "github.com/knightfall22/proglog/api/v1"
	"github.com/travisjeffery/go-dynaport"
)

func TestMultipleNodes(t *testing.T) {
	var logs []*DistributedLog
	nodeCount := 3
	ports := dynaport.Get(nodeCount)

	for i := range nodeCount {
		dataDir, err := os.MkdirTemp("", "distributed-log-test")
		if err != nil {
			t.Fatalf("error occurred making dir %v", err)
		}

		defer func() {
			os.RemoveAll(dataDir)
		}()

		ln, err := net.Listen(
			"tcp",
			fmt.Sprintf("127.0.0.1:%d", ports[i]),
		)
		if err != nil {
			t.Fatalf("couldn't start listener %v", err)
		}

		config := Config{}
		config.Raft.LocalID = raft.ServerID(fmt.Sprintf("%d", i))
		config.Raft.StreamLayer = NewStreamLayer(ln, nil, nil)
		config.Raft.HeartbeatTimeout = 50 * time.Millisecond
		config.Raft.ElectionTimeout = 50 * time.Millisecond
		config.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
		config.Raft.CommitTimeout = 5 * time.Millisecond
		config.Raft.BindAddr = ln.Addr().String()

		if i == 0 {
			config.Raft.Bootstrap = true
		}

		dl, err := NewDistributedLog(dataDir, config)
		if err != nil {
			t.Fatalf("new distributed log failed %v", err)
		}

		if i != 0 {
			err := logs[0].Join(
				fmt.Sprintf("%d", i),
				ln.Addr().String(),
			)
			if err != nil {
				t.Fatalf("error joining cluster %v", err)
			}
		} else {
			err = dl.WaitForLeader(3 * time.Second)
			if err != nil {
				t.Fatalf("error waiting for leader %v", err)
			}
		}

		logs = append(logs, dl)
	}

	records := []*proglog.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}

	for _, record := range records {
		off, err := logs[0].Append(record)
		if err != nil {
			t.Fatalf("error appending record %v", err)
		}

		sleepMs(500)

		for j := range nodeCount {
			got, err := logs[j].Read(off)
			if err != nil {
				t.Fatalf("error reading from log %v", err)
			}

			record.Offset = off
			if !reflect.DeepEqual(got.Value, record.Value) {
				t.Fatalf("value mismatch expected %v got %v", record.Value, got.Value)
			}

		}

	}

	servers, err := logs[0].GetServers()
	if err != nil {
		t.Fatalf("error getting servers %v", err)
	}

	if len(servers) != 3 {
		t.Fatalf("expected 3 server got %d", len(servers))
	}

	if !servers[0].IsLeader {
		t.Fatal("expected server 0 to the the leader.")
	}

	err = logs[0].Leave("1")
	if err != nil {
		t.Fatalf("error leaving cluster %v", err)
	}

	sleepMs(50)

	off, err := logs[0].Append(&proglog.Record{
		Value: []byte("third"),
	})
	if err != nil {
		t.Fatalf("error appending record %v", err)
	}

	record, err := logs[1].Read(off)
	if errors.Is(err, proglog.ErrOffsetOutOfRange{}) {
		t.Fatalf("error reading from log %v", err)
	}

	if record != nil {
		t.Fatalf("expected nil got %v", record)
	}

	sleepMs(50)

	record, err = logs[2].Read(off)
	if err != nil {
		t.Fatalf("error reading from log %v", err)
	}

	if !bytes.Equal(record.Value, []byte("third")) {
		t.Fatalf("value mismatch expected %v got %v", []byte("third"), record.Value)
	}

	if off != record.Offset {
		t.Fatalf("offset mismatch expected %d got %d", off, record.Offset)
	}

	servers, err = logs[0].GetServers()
	if err != nil {
		t.Fatalf("error getting servers %v", err)
	}

	if len(servers) != 2 {
		t.Fatalf("expected 2 server got %d", len(servers))
	}

	if !servers[0].IsLeader {
		t.Fatal("expected server 0 to be the leader.")
	}

	if servers[1].IsLeader {
		t.Fatal("expected server 0 to be the leader not server 1.")
	}
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}
