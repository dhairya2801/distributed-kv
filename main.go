package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"distributed-kv/engine"
	"distributed-kv/pb"
	"distributed-kv/raft"

	"google.golang.org/grpc"
)

func main() {
	id := flag.String("id", "", "node ID (e.g. node1)")
	addr := flag.String("addr", ":50051", "listen address")
	peers := flag.String("peers", "", "comma-separated peers: id1=addr1,id2=addr2")
	dataDir := flag.String("data", "data", "data directory")
	flag.Parse()

	// ── Open storage engine ────────────────────────────────
	tree, err := engine.OpenLSMTree(*dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open LSM tree: %v\n", err)
		os.Exit(1)
	}

	// ── Start gRPC server ──────────────────────────────────
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to listen on %s: %v\n", *addr, err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()

	var raftNode *raft.RaftNode

	// If peers are specified, start a Raft node.
	if *peers != "" {
		if *id == "" {
			fmt.Fprintln(os.Stderr, "--id is required when --peers is specified")
			os.Exit(1)
		}

		peerList := parsePeers(*peers)
		cfg := raft.Config{
			ID:      *id,
			DataDir: *dataDir,
			Peers:   peerList,
		}

		applyFunc := func(cmd raft.Command) error {
			switch cmd.Op {
			case raft.CmdPut:
				return tree.Put(cmd.Key, cmd.Value)
			case raft.CmdDelete:
				return tree.Delete(cmd.Key)
			default:
				return fmt.Errorf("unknown command op: %d", cmd.Op)
			}
		}

		raftNode, err = raft.NewRaftNode(cfg, applyFunc)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create raft node: %v\n", err)
			os.Exit(1)
		}

		pb.RegisterRaftServiceServer(grpcServer, raftNode)
		raftNode.Start()
		fmt.Printf("Raft node %s started with peers: %s\n", *id, *peers)
	}

	kvServer := NewKVServer(tree, raftNode)
	pb.RegisterKVServiceServer(grpcServer, kvServer)

	// Serve in a goroutine so we can wait for shutdown signal.
	go func() {
		fmt.Printf("KV server listening on %s\n", *addr)
		if err := grpcServer.Serve(lis); err != nil {
			fmt.Fprintf(os.Stderr, "gRPC serve error: %v\n", err)
		}
	}()

	// ── Wait for shutdown ──────────────────────────────────
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("\nShutting down...")
	if raftNode != nil {
		raftNode.Stop()
	}
	grpcServer.GracefulStop()
	if err := tree.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close LSM tree: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Done.")
}

// parsePeers parses "id1=addr1,id2=addr2" into PeerConfig slice.
func parsePeers(s string) []raft.PeerConfig {
	var peers []raft.PeerConfig
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		eqIdx := strings.Index(part, "=")
		if eqIdx < 0 {
			fmt.Fprintf(os.Stderr, "invalid peer format %q (expected id=addr)\n", part)
			os.Exit(1)
		}
		peers = append(peers, raft.PeerConfig{
			ID:   part[:eqIdx],
			Addr: part[eqIdx+1:],
		})
	}
	return peers
}
