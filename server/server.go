package server

import (
	"context"
	"net"
	"net/http"

	"google.golang.org/grpc"

	"github.com/celrenheit/sandglass/broker"
	"github.com/celrenheit/sandglass/logy"
	"github.com/celrenheit/sandglass/sgproto"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
)

type Server struct {
	httpAddr, grpcAddr string
	logger             logy.Logger
	server             *grpc.Server
	httpServer         *http.Server
	broker             *broker.Broker
	cancel             func()
}

func New(b *broker.Broker, grpcAddr, httpAddr string, logger logy.Logger) *Server {
	return &Server{
		httpAddr: httpAddr,
		grpcAddr: grpcAddr,
		logger:   logger,
		broker:   b,
	}
}

func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.grpcAddr)
	if err != nil {
		return err
	}
	s.server = grpc.NewServer()
	svc := newService(s.broker)
	sgproto.RegisterBrokerServiceServer(s.server, svc)

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	mux := runtime.NewServeMux(runtime.WithMarshalerOption("application/json", &runtime.JSONPb{}))

	opts := []grpc.DialOption{grpc.WithInsecure()}
	sgproto.RegisterBrokerServiceHandlerFromEndpoint(ctx, mux, s.grpcAddr, opts)

	s.httpServer = &http.Server{
		Addr:    s.httpAddr,
		Handler: mux,
	}
	go s.httpServer.ListenAndServe()

	return s.server.Serve(l)
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer != nil {
		s.httpServer.Shutdown(ctx)
	}
	if s.server != nil {
		s.server.GracefulStop()
	}
	return nil
}
