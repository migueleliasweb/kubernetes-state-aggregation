package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpcreflect"
	v1 "github.com/migueleliasweb/kubernetes-state-aggregation/pkg/api/v1"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/api/v1/v1connect"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
	"github.com/rs/cors"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// StateServer implements the Connect and gRPC StateService.
type StateServer struct {
	v1connect.UnimplementedStateServiceHandler

	fetcher datastore.Fetcher
}

// Build-time interface check.
var _ v1connect.StateServiceHandler = &StateServer{}

// NewStateServer creates a new StateServer instance.
func NewStateServer(fetcher datastore.Fetcher) *StateServer {
	return &StateServer{
		fetcher: fetcher,
	}
}

func toProtoResourceRecord(item datastore.ResourceRecord) *v1.ResourceRecord {
	var updatedAt *timestamppb.Timestamp
	if !item.UpdatedAt.IsZero() {
		updatedAt = timestamppb.New(item.UpdatedAt)
	}

	return &v1.ResourceRecord{
		Annotations:     []byte(item.Annotations),
		ClusterName:     item.ClusterName,
		GroupName:       item.GroupName,
		Kind:            item.Kind,
		Labels:          []byte(item.Labels),
		Manifest:        []byte(item.Manifest),
		Name:            item.Name,
		Namespace:       item.Namespace,
		ResourceVersion: item.ResourceVersion,
		Uid:             item.UID,
		UpdatedAt:       updatedAt,
		Version:         item.Version,
	}
}

func fromProtoResourceInfo(info *v1.ResourceInfo) datastore.ResourceInfo {
	if info == nil {
		return datastore.ResourceInfo{}
	}

	return datastore.ResourceInfo{
		ClusterName:     info.ClusterName,
		Group:           info.Group,
		Kind:            info.Kind,
		Name:            info.Name,
		Namespace:       info.Namespace,
		ResourceVersion: info.ResourceVersion,
		UID:             info.Uid,
		Version:         info.Version,
	}
}

// GetResource queries for a single resource.
func (s *StateServer) GetResource(
	ctx context.Context,
	req *connect.Request[v1.GetResourceRequest],
) (*connect.Response[v1.GetResourceResponse], error) {
	if req == nil || req.Msg == nil || req.Msg.Info == nil {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("resource info is required"),
		)
	}

	info := fromProtoResourceInfo(req.Msg.Info)
	record, err := s.fetcher.GetResource(ctx, info)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			return nil, connect.NewError(
				connect.CodeNotFound,
				errors.New("resource not found"),
			)
		}

		slog.Error(
			"failed to get resource",
			"cluster", info.ClusterName,
			"namespace", info.Namespace,
			"name", info.Name,
			"kind", info.Kind,
			"err", err,
		)

		return nil, connect.NewError(
			connect.CodeInternal,
			fmt.Errorf("failed to get resource: %w", err),
		)
	}

	return connect.NewResponse(&v1.GetResourceResponse{
		Record: toProtoResourceRecord(*record),
	}), nil
}

// ListResources queries for multiple resources matching filter.
func (s *StateServer) ListResources(
	ctx context.Context,
	req *connect.Request[v1.ListResourcesRequest],
) (*connect.Response[v1.ListResourcesResponse], error) {
	var filter datastore.ResourceInfo
	if req != nil && req.Msg != nil && req.Msg.Filter != nil {
		filter = fromProtoResourceInfo(req.Msg.Filter)
	}

	records, err := s.fetcher.ListResources(ctx, filter)
	if err != nil {
		slog.Error(
			"failed to list resources",
			"cluster", filter.ClusterName,
			"namespace", filter.Namespace,
			"name", filter.Name,
			"kind", filter.Kind,
			"err", err,
		)

		return nil, connect.NewError(
			connect.CodeInternal,
			fmt.Errorf("failed to list resources: %w", err),
		)
	}

	protoItems := []*v1.ResourceRecord{}
	for _, rec := range records {
		protoItems = append(protoItems, toProtoResourceRecord(rec))
	}

	return connect.NewResponse(&v1.ListResourcesResponse{
		Items: protoItems,
	}), nil
}

// FetchResourceGraph queries the underlying datastore for the resource graph and returns records.
func (s *StateServer) FetchResourceGraph(
	ctx context.Context,
	req *connect.Request[v1.FetchResourceGraphRequest],
) (*connect.Response[v1.FetchResourceGraphResponse], error) {
	if req == nil || req.Msg == nil || req.Msg.Root == nil {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("root resource info is required"),
		)
	}

	rootInfo := fromProtoResourceInfo(req.Msg.Root)

	collection, err := s.fetcher.FetchResourceGraph(
		ctx,
		rootInfo,
		func(resourceInfo datastore.ResourceRecord) (datastore.WalkAction, error) {
			return datastore.ActionInclude, nil
		},
	)
	if err != nil {
		slog.Error(
			"failed to fetch resource graph",
			"cluster", rootInfo.ClusterName,
			"namespace", rootInfo.Namespace,
			"name", rootInfo.Name,
			"kind", rootInfo.Kind,
			"err", err,
		)

		return nil, connect.NewError(
			connect.CodeInternal,
			fmt.Errorf("failed to fetch resource graph: %w", err),
		)
	}

	items := collection.Items()
	protoItems := []*v1.ResourceRecord{}

	for _, item := range items {
		protoItems = append(protoItems, toProtoResourceRecord(item))
	}

	return connect.NewResponse(&v1.FetchResourceGraphResponse{
		Items: protoItems,
	}), nil
}

// Option configures a Server.
type Option func(*Server)

// WithAllowedOrigins sets the CORS allowed origins.
func WithAllowedOrigins(origins []string) Option {
	return func(s *Server) {
		if len(origins) > 0 {
			s.allowedOrigins = origins
		}
	}
}

// Server wraps the Connect-Go / gRPC HTTP server and network listener.
type Server struct {
	httpServer     *http.Server
	listener       net.Listener
	allowedOrigins []string
}

// NewServer creates a new Server bound to the given listener.
func NewServer(
	fetcher datastore.Fetcher,
	listener net.Listener,
	opts ...Option,
) *Server {
	s := &Server{
		listener: listener,
		allowedOrigins: []string{
			"*",
		},
	}

	for _, opt := range opts {
		opt(s)
	}

	mux := http.NewServeMux()
	stateServer := NewStateServer(fetcher)

	path, handler := v1connect.NewStateServiceHandler(stateServer)
	mux.Handle(path, handler)

	reflector := grpcreflect.NewStaticReflector(
		v1connect.StateServiceName,
	)
	mux.Handle(grpcreflect.NewHandlerV1(reflector))
	mux.Handle(grpcreflect.NewHandlerV1Alpha(reflector))

	corsHandler := cors.New(cors.Options{
		AllowedOrigins: s.allowedOrigins,
		AllowedMethods: []string{
			http.MethodGet,
			http.MethodPost,
			http.MethodOptions,
		},
		AllowedHeaders: []string{
			"*",
		},
		ExposedHeaders: []string{
			"Grpc-Status",
			"Grpc-Message",
			"Grpc-Status-Details-Bin",
			"Connect-Protocol-Version",
			"Connect-Timeout-Ms",
		},
		AllowCredentials: true,
	}).Handler(mux)

	h2cHandler := h2c.NewHandler(corsHandler, &http2.Server{})

	s.httpServer = &http.Server{
		Handler: h2cHandler,
	}

	return s
}

// Serve starts the HTTP / gRPC server.
func (s *Server) Serve() error {
	return s.httpServer.Serve(s.listener)
}

// GracefulStop gracefully stops the server.
func (s *Server) GracefulStop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = s.httpServer.Shutdown(ctx)
}

// Stop immediately stops the server.
func (s *Server) Stop() {
	_ = s.httpServer.Close()
}
