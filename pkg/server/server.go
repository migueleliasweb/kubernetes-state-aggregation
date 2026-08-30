package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	v1 "github.com/migueleliasweb/kubernetes-state-aggregation/pkg/api/v1"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	req *v1.GetResourceRequest,
) (*v1.GetResourceResponse, error) {
	if req == nil || req.Info == nil {
		return nil, status.Error(codes.InvalidArgument, "resource info is required")
	}

	info := fromProtoResourceInfo(req.Info)
	record, err := s.fetcher.GetResource(ctx, info)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "resource not found")
		}

		slog.Error(
			"failed to get resource",
			"cluster", info.ClusterName,
			"namespace", info.Namespace,
			"name", info.Name,
			"kind", info.Kind,
			"err", err,
		)

		return nil, status.Errorf(codes.Internal, "failed to get resource: %v", err)
	}

	return &v1.GetResourceResponse{
		Record: toProtoResourceRecord(*record),
	}, nil
}

// ListResources queries for multiple resources matching filter.
func (s *StateServer) ListResources(
	ctx context.Context,
	req *v1.ListResourcesRequest,
) (*v1.ListResourcesResponse, error) {
	var filter datastore.ResourceInfo
	if req != nil && req.Filter != nil {
		filter = fromProtoResourceInfo(req.Filter)
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

		return nil, status.Errorf(codes.Internal, "failed to list resources: %v", err)
	}

	protoItems := []*v1.ResourceRecord{}
	for _, rec := range records {
		protoItems = append(protoItems, toProtoResourceRecord(rec))
	}

	return &v1.ListResourcesResponse{
		Items: protoItems,
	}, nil
}

// FetchResourceGraph queries the underlying datastore for the resource graph and returns records.
func (s *StateServer) FetchResourceGraph(
	ctx context.Context,
	req *v1.FetchResourceGraphRequest,
) (*v1.FetchResourceGraphResponse, error) {
	if req == nil || req.Root == nil {
		return nil, status.Error(codes.InvalidArgument, "root resource info is required")
	}

	rootInfo := fromProtoResourceInfo(req.Root)

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
		protoItems = append(protoItems, toProtoResourceRecord(item))
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
