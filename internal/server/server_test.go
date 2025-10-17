package server

import (
	"bytes"
	"context"
	"flag"
	"net"
	"os"
	"testing"
	"time"

	"github.com/knightfall22/proglog/internal/auth"
	"github.com/knightfall22/proglog/internal/config"
	log "github.com/knightfall22/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	proglog "github.com/knightfall22/proglog/api/v1"
	"go.opencensus.io/examples/exporter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

var debug = flag.Bool("debug", false, "Enable observability for debugging.")

type clients struct {
	Root   proglog.LogClient
	Nobody proglog.LogClient
	Health healthpb.HealthClient
}

func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		clients clients,
		config *Config,
	){
		"produce/consume a message to/from the log succeeeds": testProduceConsume,
		"produce/consume stream succeeds":                     testProduceConsumeStream,
		"consume past log boundary fails":                     testConsumePastBoundary,
		"unauthorized produce/consume fails":                  testUnauthorized,
		"healthcheck succeeds":                                testHealthCheck,
	} {
		t.Run(scenario, func(t *testing.T) {
			clients, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, clients, config)
		})
	}
}

func testProduceConsume(t *testing.T, clients clients, config *Config) {
	ctx := context.Background()

	value := proglog.Record{
		Value: []byte("hello, angel"),
	}

	produce, err := clients.Root.Produce(ctx, &proglog.ProduceRequest{
		Record: &value,
	})
	if err != nil {
		t.Fatalf("error occured while producing %v", err)
	}

	consume, err := clients.Root.Consume(ctx, &proglog.ConsumeRequest{
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

func testConsumePastBoundary(t *testing.T, clients clients, config *Config) {
	ctx := context.Background()

	produce, err := clients.Root.Produce(ctx, &proglog.ProduceRequest{
		Record: &proglog.Record{
			Value: []byte("hello, angel"),
		},
	})
	if err != nil {
		t.Fatalf("error occured while producing %v", err)
	}

	consume, err := clients.Root.Consume(ctx, &proglog.ConsumeRequest{
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
	client clients,
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

		stream, err := client.Root.ProduceStream(ctx)
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
		stream, err := client.Root.ConsumeStream(ctx,
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

func testUnauthorized(
	t *testing.T,
	client clients,
	config *Config,
) {
	ctx := context.Background()

	records := &proglog.Record{
		Value: []byte("Wilson Fisk"),
	}

	res, err := client.Nobody.Produce(ctx, &proglog.ProduceRequest{
		Record: records,
	})

	if res != nil {
		t.Fatalf("expected result to be nil got %d instead", res.Offset)
	}

	want := codes.PermissionDenied
	got := status.Code(err)

	if want != got {
		t.Fatalf("error mismatch want %v got %v", want, got)
	}

	consume, err := client.Nobody.Consume(ctx, &proglog.ConsumeRequest{
		Offset: 0,
	})

	if consume != nil {
		t.Fatalf("expected result to be nil got %v instead", consume.Record)
	}

	want = codes.PermissionDenied
	got = status.Code(err)

	if want != got {
		t.Fatalf("error mismatch want %v got %v", want, got)
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	clients clients,
	cfg *Config, teardown func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to setup listener %v", err)
	}

	newClient := func(crtPath, keyPath string) (
		*grpc.ClientConn,
		proglog.LogClient,
		[]grpc.DialOption,
	) {

		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile:      crtPath,
			KeyFile:       keyPath,
			CAFile:        config.CAFile,
			ServerAddress: listener.Addr().String(),
			Server:        false,
		})
		if err != nil {
			t.Fatalf("error as occured creating client tls config %v", err)
		}

		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}

		conn, err := grpc.NewClient(listener.Addr().String(), opts...)
		if err != nil {
			t.Fatalf("error setuping grpc client %v", err)
		}

		client := proglog.NewLogClient(conn)

		return conn, client, opts
	}

	var rootConn *grpc.ClientConn
	rootConn, clients.Root, _ = newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)

	var nobodyConn *grpc.ClientConn
	nobodyConn, clients.Nobody, _ = newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)

	clients.Health = healthpb.NewHealthClient(nobodyConn)

	dir, err := os.MkdirTemp("", "segment-test")
	if err != nil {
		t.Fatal(err)
	}

	cLog, err := log.NewLog(dir, log.Config{})
	if err != nil {
		t.Fatalf("error setting up log %v", err)
	}

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	cfg = &Config{
		CommitLog:  cLog,
		Authorizer: authorizer,
	}

	var telemetryExporter *exporter.LogExporter
	if *debug {
		metricsLogFile, err := os.CreateTemp("", "metrics-*.log")
		if err != nil {
			t.Fatalf("error creating metrics log file %v", err)
		}

		t.Logf("metrics log file: %s", metricsLogFile.Name())
		tracesLogFile, err := os.CreateTemp("", "traces-*.log")
		if err != nil {
			t.Fatalf("error creating traces log file %v", err)
		}
		t.Logf("traces log file: %s", tracesLogFile.Name())

		telemetryExporter, err = exporter.NewLogExporter(exporter.Options{
			MetricsLogFile:    metricsLogFile.Name(),
			TracesLogFile:     tracesLogFile.Name(),
			ReportingInterval: time.Second,
		})
		if err != nil {
			t.Fatalf("error creating telemetry exporter %v", err)
		}

		err = telemetryExporter.Start()
		if err != nil {
			t.Fatalf("error creating telemetry exporter %v", err)
		}
	}

	if fn != nil {
		fn(cfg)
	}

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: listener.Addr().String(),
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
		server.Serve(listener)
	}()

	return clients, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		listener.Close()
		if telemetryExporter != nil {
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Stop()
			telemetryExporter.Close()
		}
		cLog.Remove()
	}
}

func testHealthCheck(
	t *testing.T,
	clients clients,
	config *Config,
) {
	ctx := context.Background()
	res, err := clients.Health.Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, res.Status)
}
