package main

import (
	"context"
	"net"
	"testing"
	"time"

	"distributed-kv/engine"
	"distributed-kv/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// startTestServer spins up a gRPC KV server on a random port and returns
// a connected client plus a cleanup function.
func startTestServer(t *testing.T) (pb.KVServiceClient, func()) {
	t.Helper()

	dir := tempDir(t) // from lsmtree_test.go
	tree, err := engine.OpenLSMTree(dir)
	if err != nil {
		t.Fatal(err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	srv := grpc.NewServer()
	pb.RegisterKVServiceServer(srv, NewKVServer(tree, nil))
	go srv.Serve(lis)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatal(err)
	}

	client := pb.NewKVServiceClient(conn)
	cleanup := func() {
		conn.Close()
		srv.GracefulStop()
		tree.Close()
	}
	return client, cleanup
}

func TestGRPCPutGetDelete(t *testing.T) {
	client, cleanup := startTestServer(t)
	defer cleanup()
	ctx := context.Background()

	// Put
	_, err := client.Put(ctx, &pb.PutRequest{Key: []byte("k1"), Value: []byte("v1")})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Get — found
	resp, err := client.Get(ctx, &pb.GetRequest{Key: []byte("k1")})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !resp.Found || string(resp.Value) != "v1" {
		t.Fatalf("Get(k1) = %q, found=%v; want v1, true", resp.Value, resp.Found)
	}

	// Get — not found
	resp, err = client.Get(ctx, &pb.GetRequest{Key: []byte("k2")})
	if err != nil {
		t.Fatalf("Get(k2): %v", err)
	}
	if resp.Found {
		t.Fatal("expected k2 not found")
	}

	// Delete
	_, err = client.Delete(ctx, &pb.DeleteRequest{Key: []byte("k1")})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Get after delete — not found
	resp, err = client.Get(ctx, &pb.GetRequest{Key: []byte("k1")})
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if resp.Found {
		t.Fatal("expected k1 not found after delete")
	}
}

func TestGRPCEmptyKeyRejected(t *testing.T) {
	client, cleanup := startTestServer(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.Put(ctx, &pb.PutRequest{Key: nil, Value: []byte("v")})
	if err == nil {
		t.Fatal("expected error for empty key Put")
	}

	_, err = client.Get(ctx, &pb.GetRequest{Key: nil})
	if err == nil {
		t.Fatal("expected error for empty key Get")
	}

	_, err = client.Delete(ctx, &pb.DeleteRequest{Key: nil})
	if err == nil {
		t.Fatal("expected error for empty key Delete")
	}
}

func TestConnPool(t *testing.T) {
	// Spin up a real server to test the pool against.
	client_unused, cleanup := startTestServer(t)
	_ = client_unused
	defer cleanup()

	// We need the address — extract from the cleanup's server.
	// Simpler: just start a minimal listener.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	go srv.Serve(lis)
	defer srv.GracefulStop()

	pool := NewConnPool(lis.Addr().String(), 2, 3*time.Second)
	defer pool.Close()

	ctx := context.Background()

	// Acquire two connections (fills the pool).
	c1, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire 1: %v", err)
	}
	c2, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire 2: %v", err)
	}

	// Third acquire should block; use a short timeout.
	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = pool.Acquire(shortCtx)
	if err == nil {
		t.Fatal("expected timeout on third Acquire with pool size 2")
	}

	// Release one, then acquire should succeed.
	pool.Release(c1)
	c3, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire after release: %v", err)
	}
	pool.Release(c2)
	pool.Release(c3)
}
