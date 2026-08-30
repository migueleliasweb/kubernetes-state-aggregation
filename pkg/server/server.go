package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	v1 "github.com/migueleliasweb/kubernetes-state-aggregation/pkg/api/v1"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// StateServer implements the gRPC StateService.
type StateServer struct {
	v1.UnimplementedStateServiceServer

	fetcher datastore.Fetcher
}

// Build-time interface check.
var _ v1.StateServiceServer = &StateServer{}

// NewStateServer creates a new StateServer instance.
func NewStateServer(fetcher datastore.Fetcher) *StateServer {
	return &StateServer{
		fetcher: fetcher,
	}
}

// FetchResourceGraph queries the underlying datastore for the resource graph and returns records.
func (s *StateServer) FetchResourceGraph(
	ctx context.Context,
	req *v1.FetchResourceGraphRequest,
) (*v1.FetchResourceGraphResponse, error) {
	if req == nil || req.Root == nil {
		return nil, fmt.Errorf("root resource info is required")
	}

	rootInfo := datastore.ResourceInfo{
		ClusterName:     req.Root.ClusterName,
		Group:           req.Root.Group,
		Kind:            req.Root.Kind,
		Name:            req.Root.Name,
		Namespace:       req.Root.Namespace,
		ResourceVersion: req.Root.ResourceVersion,
		UID:             req.Root.Uid,
		Version:         req.Root.Version,
	}

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

		return nil, fmt.Errorf("failed to fetch resource graph: %w", err)
	}

	items := collection.Items()
	protoItems := []*v1.ResourceRecord{}

	for _, item := range items {
		var updatedAt *timestamppb.Timestamp
		if !item.UpdatedAt.IsZero() {
			updatedAt = timestamppb.New(item.UpdatedAt)
		}

		protoItems = append(protoItems, &v1.ResourceRecord{
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
		})
	}

	return &v1.FetchResourceGraphResponse{
		Items: protoItems,
	}, nil
}

// Server wraps the gRPC server and network listener.
type Server struct {
	grpcServer *grpc.Server
	listener   net.Listener
}

// NewServer creates a new Server bound to the given listener.
func NewServer(
	fetcher datastore.Fetcher,
	listener net.Listener,
) *Server {
	grpcServer := grpc.NewServer()
	stateServer := NewStateServer(fetcher)

	v1.RegisterStateServiceServer(grpcServer, stateServer)

	return &Server{
		grpcServer: grpcServer,
		listener:   listener,
	}
}

// Serve starts the gRPC server.
func (s *Server) Serve() error {
	return s.grpcServer.Serve(s.listener)
}

// GracefulStop gracefully stops the gRPC server.
func (s *Server) GracefulStop() {
	s.grpcServer.GracefulStop()
}

// Stop immediately stops the gRPC server.
func (s *Server) Stop() {
	s.grpcServer.Stop()
}
