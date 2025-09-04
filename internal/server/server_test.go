package server

import (
	"bytes"
	"context"
	"net"
	"os"
	"testing"

	"github.com/knightfall22/proglog/internal/config"
	log "github.com/knightfall22/proglog/internal/log"

	proglog "github.com/knightfall22/proglog/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client proglog.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeeds": testProduceConsume,
		"produce/consume stream succeeds":                     testProduceConsumeStream,
		"consume past log boundary fails":                     testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func testProduceConsume(t *testing.T, client proglog.LogClient, config *Config) {
	ctx := context.Background()

	value := proglog.Record{
		Value: []byte("hello, angel"),
	}

	produce, err := client.Produce(ctx, &proglog.ProduceRequest{
		Record: &value,
	})
	if err != nil {
		t.Fatalf("error occured while producing %v", err)
	}

	consume, err := client.Consume(ctx, &proglog.ConsumeRequest{
		Offset: produce.Offset,
	})
	if err != nil {
		t.Fatalf("error occured while consuming %v", err)
	}

	if !bytes.Equal(consume.Record.Value, value.Value) {
		t.Fatal("Consume Value mismatch")
	}

	if consume.Record.Offset != value.Offset {
		t.Fatal("Consume offset mismatch")
	}
}

func testConsumePastBoundary(t *testing.T, client proglog.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &proglog.ProduceRequest{
		Record: &proglog.Record{
			Value: []byte("hello, angel"),
		},
	})
	if err != nil {
		t.Fatalf("error occured while producing %v", err)
	}

	consume, err := client.Consume(ctx, &proglog.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatal("cosume is not nil")
	}

	got := status.Code(err)
	want := status.Code(proglog.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}

}

func testProduceConsumeStream(
	t *testing.T,
	client proglog.LogClient,
	config *Config,
) {
	ctx := context.Background()

	records := []*proglog.Record{
		{
			Value:  []byte("Wilson Fisk"),
			Offset: 0,
		},
		{
			Value:  []byte("Lex Luthor"),
			Offset: 1,
		},
	}
	{

		stream, err := client.ProduceStream(ctx)
		if err != nil {
			t.Fatalf("Failed starting up produce stream: %v\n", err)
		}

		for offset, record := range records {
			err = stream.Send(&proglog.ProduceRequest{
				Record: record,
			})
			if err != nil {
				t.Fatalf("error sending stream %v", err)
			}

			res, err := stream.Recv()
			if err != nil {
				t.Fatalf("error receiving stream %v", err)
			}

			if res.Offset != uint64(offset) {
				t.Fatalf(
					"got offset: %d, want: %d",
					res.Offset,
					offset,
				)
			}
		}
	}
	{
		stream, err := client.ConsumeStream(ctx,
			&proglog.ConsumeRequest{Offset: 0},
		)
		if err != nil {
			t.Fatal("failed to start consume stream")
		}

		for i, record := range records {
			res, err := stream.Recv()
			if err != nil {
				t.Fatalf("error receiving stream %v", err)
			}

			if res.Record.Offset != uint64(i) {
				t.Fatalf(
					"got offset: %d, want: %d",
					res.Record.Offset,
					i,
				)
			}

			if !bytes.Equal(res.Record.Value, record.Value) {
				t.Fatalf(
					"got value: %s, want: %s",
					res.Record.Value,
					record.Value,
				)
			}
		}
	}
}

func setupTest(t *testing.T, fn func(*Config)) (client proglog.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to setup listener %v", err)
	}

	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile:   config.CAFile,
		CertFile: config.ClientCertFile,
		KeyFile:  config.ClientKeyFile,
	})
	if err != nil {
		t.Fatalf("failed to setup tls config %v", err)
	}

	clientCreds := credentials.NewTLS(clientTLSConfig)

	clientOpts := []grpc.DialOption{grpc.WithTransportCredentials(clientCreds)}

	cc, err := grpc.NewClient(l.Addr().String(), clientOpts...)
	if err != nil {
		t.Fatalf("error setuping grpc client %v", err)
	}

	dir, err := os.MkdirTemp("", "segment-test")
	if err != nil {
		t.Fatal(err)
	}

	cLog, err := log.NewLog(dir, log.Config{})
	if err != nil {
		t.Fatalf("error setting up log %v", err)
	}

	cfg = &Config{
		CommitLog: cLog,
	}

	if fn != nil {
		fn(cfg)
	}

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	if err != nil {
		t.Fatalf("failed to setup tls config %v", err)
	}

	serverTLSCreds := credentials.NewTLS(serverTLSConfig)

	server, err := NewGRPCServer(cfg, grpc.Creds(serverTLSCreds))
	if err != nil {
		t.Fatalf("error setting up grpc server %v", err)
	}

	go func() {
		server.Serve(l)
	}()

	client = proglog.NewLogClient(cc)

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		cLog.Remove()
	}
}
