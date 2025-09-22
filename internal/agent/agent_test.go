package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"

	proglog "github.com/knightfall22/proglog/api/v1"
	"github.com/knightfall22/proglog/internal/config"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestAgent(t *testing.T) {
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})

	if err != nil {
		t.Fatalf("error setting up tls config %v", err)
	}

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})

	if err != nil {
		t.Fatalf("error setting up tls client config %v", err)
	}

	var agents []*Agent
	for i := range 3 {
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		dataDir, err := os.MkdirTemp("", "agent-test-log")
		if err != nil {
			t.Fatalf("error creating dir %v", err)
		}

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].BindAddr)
		}

		agent, err := New(Config{
			NodeName:        fmt.Sprintf("%d", i),
			StartJoinAddrs:  startJoinAddrs,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			DataDir:         dataDir,
		})
		if err != nil {
			t.Fatalf("cannot create agent %v", err)
		}

		agents = append(agents, agent)

		defer func() {
			for _, agent := range agents {
				if err := agent.Shutdown(); err != nil {
					t.Fatalf("error shutting down %v", err)
				}

				if err := os.RemoveAll(agent.DataDir); err != nil {
					t.Fatalf("error remving dir %v", err)
				}
			}
		}()
	}

	time.Sleep(3 * time.Second)

	leaderClient := client(t, agents[0], peerTLSConfig)
	produceResp, err := leaderClient.Produce(context.Background(),
		&proglog.ProduceRequest{
			Record: &proglog.Record{
				Value: []byte("Hello"),
			},
		},
	)
	if err != nil {
		t.Fatalf("cannot produce record %v", err)
	}

	consumeResp, err := leaderClient.Consume(
		context.Background(),
		&proglog.ConsumeRequest{
			Offset: produceResp.Offset,
		},
	)

	if err != nil {
		t.Fatalf("cannot consume record %v", err)
	}

	if !bytes.Equal(consumeResp.Record.Value, []byte("Hello")) {
		t.Fatalf("Value do not match expected: %s got: %s", []byte("Hello"), consumeResp.Record.Value)
	}
}

func client(
	t *testing.T,
	agent *Agent,
	tlsConfig *tls.Config,
) proglog.LogClient {
	tlsClient := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsClient)}
	rpcAddr, err := agent.RPCAddr()
	if err != nil {
		t.Fatalf("error getting rpc addr %v", err)
	}

	conn, err := grpc.NewClient(rpcAddr, opts...)
	if err != nil {
		t.Fatalf("error dialing %v", err)
	}
	if err != nil {
		t.Fatalf("error dialing %v", err)
	}

	client := proglog.NewLogClient(conn)
	return client
}
