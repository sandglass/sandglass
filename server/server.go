package server

import (
	"context"
	"net"
	"net/http"
	"sync"

	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/celrenheit/sandglass/broker"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/sirupsen/logrus"
)

type Server struct {
	httpAddr, grpcAddr string
	logger             *logrus.Entry
	server             *grpc.Server
	httpServer         *http.Server
	broker             *broker.Broker
	cancel             func()
	lis                net.Listener
	wg                 sync.WaitGroup
}

func New(b *broker.Broker, grpcAddr, httpAddr string) *Server {
	return &Server{
		httpAddr: httpAddr,
		grpcAddr: grpcAddr,
		logger:   b.Entry,
		broker:   b,
	}
}

func (s *Server) Start() (err error) {
	s.lis, err = net.Listen("tcp", s.grpcAddr)
	if err != nil {
		return err
	}
	s.server = grpc.NewServer()
	svc := newService(s.broker)
	sgproto.RegisterBrokerServiceServer(s.server, svc)
	sgproto.RegisterInternalServiceServer(s.server, svc)

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	mux := runtime.NewServeMux(runtime.WithMarshalerOption("application/json", &runtime.JSONPb{}))

	opts := []grpc.DialOption{grpc.WithInsecure()}
	sgproto.RegisterBrokerServiceHandlerFromEndpoint(ctx, mux, s.grpcAddr, opts)

	s.httpServer = &http.Server{
		Addr:    s.httpAddr,
		Handler: mux,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.httpServer.ListenAndServe()
	}()

	return s.server.Serve(s.lis)
}

func (s *Server) Shutdown(ctx context.Context) (err error) {
	if s.cancel != nil {
		s.cancel()
	}
	if s.httpServer != nil {
		s.httpServer.Shutdown(ctx)
	}
	if s.server != nil {
		s.server.GracefulStop()
	}
	if s.lis != nil {
		s.lis.Close()
	}

	s.wg.Wait()
	return
}
