package discovery

import (
	"net"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

type Config struct {
	//This is a unique name for the agent.
	NodeName string
	//This is the address and port used for communication between Serf agents in a cluster.
	BindAddr string

	// Serf shares these tags to the other nodes in the cluster and should
	// use these tags for simple data that informs the cluster how to handle this
	// node. For example, Consul shares each node’s RPC address with Serf
	// tags, and once they know each other’s RPC address, they can make RPCs
	// to each other. Consul shares whether the node is a voter or non-voter,
	// which changes the node’s role in the Raft cluster.
	// In our code, similar to Consul, we’ll share each node’s user-configured
	// RPC address with a Serf tag so the nodes know which addresses
	// to send their RPCs
	Tags map[string]string

	// When you have an existing cluster and you create a new node
	// that you want to add to that cluster, you need to point your new node to
	// at least one of the nodes now in the cluster. After the new node connects
	// to one of those nodes in the existing cluster, it’ll learn about the rest of
	// the nodes, and vice versa (the existing nodes learn about the new node).
	// The StartJoinAddrs field is how you configure new nodes to join an existing
	// cluster. You set the field to the addresses of nodes in the cluster, and
	// Serf’s gossip protocol takes care of the rest to join your node to the cluster.
	// In a production environment, specify at least three addresses to make
	// your cluster resilient to one or two node failures or a disrupted network.
	StartJoinAddrs []string
}

// Handler represents some component in our service that needs to know
// when a server joins or leaves the cluster.
type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	//the event channel is how you’ll receive Serf’s events; node joins or leaves
	events chan serf.Event
	logger *zap.Logger
}

func New(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}

	if err := c.setupSerf(); err != nil {
		return nil, err
	}

	return c, nil
}

func (m *Membership) setupSerf() error {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}

	config := serf.DefaultConfig()
	config.Init()
	//Serf listens on this address and port for gossiping.
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port

	m.events = make(chan serf.Event)
	config.EventCh = m.events
	config.Tags = m.Tags
	//the node name acts as the node’s unique identifier across the Serf cluster.
	config.NodeName = m.Config.NodeName
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	go m.eventHandler()
	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// Listens for serf events and processes them appropriately
// depending on the type of event; join, leave or fail.
func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}

		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					return
				}
				m.handleLeave(member)
			}
		}
	}
}

// Joins serf.Member to existing node use a class that fufills the handler interface
func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(
		member.Name,
		member.Tags["rpc_addr"],
	); err != nil {
		m.logError(err, "failed to join", member)
	}
}

// Remove serf.Member to existing node use a class that fufills the handler interface
func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(
		member.Name,
	); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}
func (m *Membership) Leave() error {
	return m.serf.Leave()
}
func (m *Membership) logError(err error, msg string, member serf.Member) {
	log := m.logger.Error
	if err == raft.ErrNotLeader {
		log = m.logger.Debug
	}
	log(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
