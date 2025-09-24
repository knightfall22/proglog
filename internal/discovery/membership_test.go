package discovery

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/travisjeffery/go-dynaport"
)

type handler struct {
	joins  chan map[string]string
	leaves chan string
}

func (h *handler) Join(id, addr string) error {
	if h.joins != nil {
		h.joins <- map[string]string{
			"id":   id,
			"addr": addr,
		}
	}
	return nil
}

func (h *handler) Leave(id string) error {
	if h.leaves != nil {
		h.leaves <- id
	}
	return nil
}

func TestMemberShip(t *testing.T) {
	m, handler := setupMember(t, nil)
	m, _ = setupMember(t, m)
	m, _ = setupMember(t, m)

	sleepMs(3000)

	if len(handler.joins) != 2 &&
		len(m[0].Members()) != 3 &&
		len(handler.leaves) != 0 {
		t.Fatal("invalid condition")
	}

	if err := m[2].Leave(); err != nil {
		t.Fatalf("error has occured while leaving %v", err)
	}

	if len(handler.joins) != 2 &&
		len(m[0].Members()) != 3 &&
		serf.StatusLeft != m[0].Members()[2].Status &&
		len(handler.leaves) != 1 {
		t.Fatal("invalid condition")
	}
}

func setupMember(t *testing.T, members []*Membership) ([]*Membership, *handler) {
	id := len(members)
	port := dynaport.Get(1)
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port[0])
	tags := map[string]string{
		"rpc_addr": addr,
	}

	c := Config{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addr,
		Tags:     tags,
	}

	h := &handler{}
	if len(members) == 0 {
		h.joins = make(chan map[string]string, 3)
		h.leaves = make(chan string, 3)
	} else {
		c.StartJoinAddrs = []string{
			members[0].BindAddr,
		}
	}

	m, err := New(h, c)
	if err != nil {
		t.Fatalf("an error has occurred instantiating membership %v", err)
	}

	members = append(members, m)
	return members, h
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}
