package server

import (
	"context"

	proglog "github.com/knightfall22/proglog/api/v1"
	"google.golang.org/grpc"
)

type Config struct {
	CommitLog CommitLog
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

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
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
	off, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}

	return &proglog.ProduceResponse{Offset: off}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *proglog.ConsumeRequest) (*proglog.ConsumeResponse, error) {
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
