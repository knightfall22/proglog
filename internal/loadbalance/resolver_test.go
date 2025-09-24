package loadbalance

import (
	"net"
	"net/url"
	"testing"

	proglog "github.com/knightfall22/proglog/api/v1"
	"github.com/knightfall22/proglog/internal/config"
	"github.com/knightfall22/proglog/internal/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

func TestResolver(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "127.0.0.1",
		Server:        true,
	})

	if err != nil {
		t.Fatalf("error as occured creating client tls config %v", err)
	}

	serverCreds := credentials.NewTLS(tlsConfig)

	srv, err := server.NewGRPCServer(&server.Config{
		GetServerer: &getServers{},
	}, grpc.Creds(serverCreds))

	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	go srv.Serve(l)

	conn := &clientConn{}
	tlsConfig, err = config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "127.0.0.1",
		Server:        false,
	})

	if err != nil {
		t.Fatalf("error as occured creating client tls config %v", err)
	}

	serverCreds = credentials.NewTLS(tlsConfig)
	opts := resolver.BuildOptions{
		DialCreds: serverCreds,
	}

	r := &Resolver{}

	url := ListenerToUrl(l)

	_, err = r.Build(
		resolver.Target{
			URL: url,
		},
		conn, opts)

	if err != nil {
		t.Fatalf("error as occured building resolver %v", err)
	}

	wantState := resolver.State{
		Addresses: []resolver.Address{{
			Addr:       "localhost:9001",
			Attributes: attributes.New("is_leader", true),
		}, {
			Addr:       "localhost:9002",
			Attributes: attributes.New("is_leader", false),
		}},
	}
	require.Equal(t, wantState, conn.state)

	conn.state.Addresses = nil
	r.ResolveNow(resolver.ResolveNowOptions{})
	require.Equal(t, wantState, conn.state)
}

func ListenerToUrl(l net.Listener) url.URL {
	return url.URL{Scheme: "proglog", Path: "/" + l.Addr().String()}
}

type getServers struct{}

func (s *getServers) GetServers() ([]*proglog.Server, error) {
	return []*proglog.Server{{
		Id:       "leader",
		RpcAddr:  "localhost:9001",
		IsLeader: true,
	}, {
		Id:      "follower",
		RpcAddr: "localhost:9002",
	}}, nil
}

type clientConn struct {
	resolver.ClientConn
	state resolver.State
}

func (c *clientConn) UpdateState(state resolver.State) error {
	c.state = state
	return nil
}
func (c *clientConn) ReportError(err error)               {}
func (c *clientConn) NewAddress(addrs []resolver.Address) {}
func (c *clientConn) NewServiceConfig(config string)      {}
func (c *clientConn) ParseServiceConfig(
	config string,
) *serviceconfig.ParseResult {
	return nil
}
