package server

import (
	"context"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	proglog "github.com/knightfall22/proglog/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

var _ proglog.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	proglog.UnimplementedLogServer
	*Config
}

type CommitLog interface {
	Append(*proglog.Record) (uint64, error)
	Read(uint64) (*proglog.Record, error)
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

func authenticate(ctx context.Context) (context.Context, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldn't find peer info",
		).Err()
	}

	if p.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := p.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}

// Custom sampler that always logs produce requests and 50% of all other requests
// halfSampler := trace.ProbabilitySampler(0.5)
// trace.ApplyConfig(trace.Config{
// DefaultSampler: func(p trace.SamplingParameters) trace.SamplingDecision {
// if strings.Contains(p.Name, "Produce"){
// return trace.SamplingDecision{Sample: true}
// }
// return halfSampler(p)
// },
// })
func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	logger := zap.L().Named("server")
	zapOpts := []grpc_zap.Option{
		grpc_zap.WithDurationField(
			func(duration time.Duration) zapcore.Field {
				return zap.Int64("grpc.time_ns", duration.Nanoseconds())
			},
		),
	}

	//We’ve configured OpenCensus to always sample the traces because we’re
	// developing our service and we want all of our requests traced.
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	if err := view.Register(ocgrpc.DefaultServerViews...); err != nil {
		return nil, err
	}

	opts = append(opts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_ctxtags.StreamServerInterceptor(),
				grpc_zap.StreamServerInterceptor(logger, zapOpts...),
				grpc_auth.StreamServerInterceptor(authenticate),
			)), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
			grpc_auth.UnaryServerInterceptor(authenticate),
		)),
		grpc.StatsHandler(&ocgrpc.ServerHandler{}),
	)
	grsrv := newgrpcServer(config)
	srv := grpc.NewServer(opts...)

	proglog.RegisterLogServer(srv, grsrv)

	return srv, nil
}

func newgrpcServer(c *Config) *grpcServer {
	return &grpcServer{
		Config: c,
	}
}

func (s *grpcServer) Produce(ctx context.Context, req *proglog.ProduceRequest) (*proglog.ProduceResponse, error) {
	if err := s.Authorizer.Authorize(subject(ctx), objectWildcard, produceAction); err != nil {
		return nil, err
	}

	off, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}

	return &proglog.ProduceResponse{Offset: off}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *proglog.ConsumeRequest) (*proglog.ConsumeResponse, error) {
	if err := s.Authorizer.Authorize(subject(ctx), objectWildcard, consumeAction); err != nil {
		return nil, err
	}

	rec, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}

	return &proglog.ConsumeResponse{Record: rec}, nil
}

func (s *grpcServer) ProduceStream(
	stream proglog.Log_ProduceStreamServer,
) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}

		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(
	req *proglog.ConsumeRequest,
	stream proglog.Log_ConsumeStreamServer,
) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil

		default:
			res, err := s.Consume(stream.Context(), req)

			switch err.(type) {
			case nil:
			case proglog.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}

			if err := stream.Send(res); err != nil {
				return nil
			}

			req.Offset++
		}
	}
}
