package server

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/migueleliasweb/kubernetes-state-aggregation/pkg/api/v1"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/api/v1/v1connect"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type mockFetcher struct {
	collection *datastore.UniqueResourceCollection
	records    []datastore.ResourceRecord
	err        error
}

// Build-time interface check.
var _ datastore.Fetcher = (*mockFetcher)(nil)

func (m *mockFetcher) FetchResourceGraph(
	ctx context.Context,
	rootResourceInfo datastore.ResourceInfo,
	callback datastore.ResourceCallback,
) (*datastore.UniqueResourceCollection, error) {
	if m.err != nil {
		return nil, m.err
	}

	return m.collection, nil
}

func (m *mockFetcher) GetResource(
	ctx context.Context,
	info datastore.ResourceInfo,
) (*datastore.ResourceRecord, error) {
	if m.err != nil {
		return nil, m.err
	}

	for _, rec := range m.records {
		if (info.UID == "" || rec.UID == info.UID) &&
			(info.ClusterName == "" || rec.ClusterName == info.ClusterName) &&
			(info.Kind == "" || rec.Kind == info.Kind) &&
			(info.Namespace == "" || rec.Namespace == info.Namespace) &&
			(info.Name == "" || rec.Name == info.Name) {
			return &rec, nil
		}
	}

	return nil, datastore.ErrNotFound
}

func (m *mockFetcher) ListResources(
	ctx context.Context,
	filter datastore.ResourceInfo,
) ([]datastore.ResourceRecord, error) {
	if m.err != nil {
		return nil, m.err
	}

	var results []datastore.ResourceRecord
	for _, rec := range m.records {
		if (filter.UID == "" || rec.UID == filter.UID) &&
			(filter.ClusterName == "" || rec.ClusterName == filter.ClusterName) &&
			(filter.Kind == "" || rec.Kind == filter.Kind) &&
			(filter.Namespace == "" || rec.Namespace == filter.Namespace) &&
			(filter.Name == "" || rec.Name == filter.Name) {
			results = append(results, rec)
		}
	}

	return results, nil
}

func TestStateServer_FetchResourceGraph(t *testing.T) {
	collection := datastore.NewUniqueResourceCollection()
	now := time.Now().UTC().Truncate(time.Second)

	collection.Add(datastore.ResourceRecord{
		ClusterName:     "c1",
		GroupName:       "apps",
		Kind:            "Deployment",
		Manifest:        []byte(`{"apiVersion":"apps/v1"}`),
		Name:            "web",
		Namespace:       "default",
		ResourceVersion: "123",
		UID:             "uid-1",
		UpdatedAt:       now,
		Version:         "v1",
	})

	fetcher := &mockFetcher{
		collection: collection,
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	srv := NewServer(fetcher, lis)
	go func() {
		_ = srv.Serve()
	}()
	defer srv.GracefulStop()

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := v1.NewStateServiceClient(conn)

	res, err := client.FetchResourceGraph(context.Background(), &v1.FetchResourceGraphRequest{
		Root: &v1.ResourceInfo{
			ClusterName: "c1",
			Kind:        "Deployment",
			Name:        "web",
			Namespace:   "default",
		},
	})
	if err != nil {
		t.Fatalf("FetchResourceGraph failed: %v", err)
	}

	if len(res.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(res.Items))
	}

	item := res.Items[0]
	if item.Uid != "uid-1" {
		t.Errorf("expected UID 'uid-1', got %q", item.Uid)
	}
	if item.Kind != "Deployment" {
		t.Errorf("expected Kind 'Deployment', got %q", item.Kind)
	}
	if item.Name != "web" {
		t.Errorf("expected Name 'web', got %q", item.Name)
	}
	if string(item.Manifest) != `{"apiVersion":"apps/v1"}` {
		t.Errorf("expected manifest match, got %s", string(item.Manifest))
	}
	if item.UpdatedAt.AsTime().Unix() != now.Unix() {
		t.Errorf("expected UpdatedAt %v, got %v", now, item.UpdatedAt.AsTime())
	}
}

func TestStateServer_GetAndListResources(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	fetcher := &mockFetcher{
		records: []datastore.ResourceRecord{
			{
				ClusterName:     "c1",
				GroupName:       "apps",
				Kind:            "Deployment",
				Manifest:        []byte(`{"apiVersion":"apps/v1","metadata":{"name":"web"}}`),
				Name:            "web",
				Namespace:       "default",
				ResourceVersion: "123",
				UID:             "uid-1",
				UpdatedAt:       now,
				Version:         "v1",
			},
			{
				ClusterName:     "c2",
				GroupName:       "apps",
				Kind:            "Deployment",
				Manifest:        []byte(`{"apiVersion":"apps/v1","metadata":{"name":"web"}}`),
				Name:            "web",
				Namespace:       "default",
				ResourceVersion: "456",
				UID:             "uid-2",
				UpdatedAt:       now,
				Version:         "v1",
			},
		},
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	srv := NewServer(fetcher, lis)
	go func() {
		_ = srv.Serve()
	}()
	defer srv.GracefulStop()

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := v1.NewStateServiceClient(conn)

	// Test GetResource success
	getRes, err := client.GetResource(context.Background(), &v1.GetResourceRequest{
		Info: &v1.ResourceInfo{
			ClusterName: "c1",
			Kind:        "Deployment",
			Name:        "web",
			Namespace:   "default",
		},
	})
	if err != nil {
		t.Fatalf("GetResource failed: %v", err)
	}
	if getRes.Record.Uid != "uid-1" {
		t.Errorf("expected UID 'uid-1', got %q", getRes.Record.Uid)
	}

	// Test GetResource NotFound
	_, err = client.GetResource(context.Background(), &v1.GetResourceRequest{
		Info: &v1.ResourceInfo{
			ClusterName: "c3",
			Kind:        "Deployment",
			Name:        "non-existent",
		},
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound code, got %v", status.Code(err))
	}

	// Test ListResources
	listRes, err := client.ListResources(context.Background(), &v1.ListResourcesRequest{
		Filter: &v1.ResourceInfo{
			Kind: "Deployment",
		},
	})
	if err != nil {
		t.Fatalf("ListResources failed: %v", err)
	}
	if len(listRes.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(listRes.Items))
	}
}

func TestStateServer_ConnectClient(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	fetcher := &mockFetcher{
		records: []datastore.ResourceRecord{
			{
				ClusterName:     "c1",
				GroupName:       "apps",
				Kind:            "Deployment",
				Manifest:        []byte(`{"apiVersion":"apps/v1","metadata":{"name":"web"}}`),
				Name:            "web",
				Namespace:       "default",
				ResourceVersion: "123",
				UID:             "uid-1",
				UpdatedAt:       now,
				Version:         "v1",
			},
		},
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	srv := NewServer(fetcher, lis)
	go func() {
		_ = srv.Serve()
	}()
	defer srv.GracefulStop()

	client := v1connect.NewStateServiceClient(
		&http.Client{},
		"http://"+lis.Addr().String(),
	)

	// Test GetResource via Connect
	res, err := client.GetResource(
		context.Background(),
		connect.NewRequest(&v1.GetResourceRequest{
			Info: &v1.ResourceInfo{
				ClusterName: "c1",
				Kind:        "Deployment",
				Name:        "web",
				Namespace:   "default",
			},
		}),
	)
	if err != nil {
		t.Fatalf("Connect GetResource failed: %v", err)
	}
	if res.Msg.Record.Uid != "uid-1" {
		t.Errorf("expected UID 'uid-1', got %q", res.Msg.Record.Uid)
	}

	// Test NotFound via Connect
	_, err = client.GetResource(
		context.Background(),
		connect.NewRequest(&v1.GetResourceRequest{
			Info: &v1.ResourceInfo{
				ClusterName: "c99",
				Kind:        "Deployment",
				Name:        "missing",
			},
		}),
	)
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("expected CodeNotFound, got %v", connect.CodeOf(err))
	}
}
