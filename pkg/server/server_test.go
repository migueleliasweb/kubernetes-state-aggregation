package server

import (
	"context"
	"net"
	"testing"
	"time"

	v1 "github.com/migueleliasweb/kubernetes-state-aggregation/pkg/api/v1"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type mockFetcher struct {
	collection *datastore.UniqueResourceCollection
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
