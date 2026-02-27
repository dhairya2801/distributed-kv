package main

import (
	"context"
	"errors"

	"distributed-kv/engine"
	"distributed-kv/pb"
	"distributed-kv/raft"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KVServer implements the gRPC KVServiceServer interface. It delegates all
// operations to a local LSMTree. When raftNode is non-nil, writes go
// through Raft consensus; reads stay local.
type KVServer struct {
	pb.UnimplementedKVServiceServer
	tree     *engine.LSMTree
	raftNode *raft.RaftNode
}

// NewKVServer creates a KVServer backed by the given LSMTree.
// Pass nil for raftNode to run in standalone mode.
func NewKVServer(tree *engine.LSMTree, raftNode *raft.RaftNode) *KVServer {
	return &KVServer{tree: tree, raftNode: raftNode}
}

func (s *KVServer) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	val, found, err := s.tree.Get(req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	return &pb.GetResponse{Value: val, Found: found}, nil
}

func (s *KVServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	if s.raftNode != nil {
		err := s.raftNode.Propose(ctx, raft.Command{Op: raft.CmdPut, Key: req.Key, Value: req.Value})
		if err != nil {
			var nle *raft.NotLeaderError
			if errors.As(err, &nle) {
				return nil, status.Errorf(codes.FailedPrecondition, "%v", nle)
			}
			return nil, status.Errorf(codes.Internal, "raft put: %v", err)
		}
		return &pb.PutResponse{}, nil
	}

	if err := s.tree.Put(req.Key, req.Value); err != nil {
		return nil, status.Errorf(codes.Internal, "put: %v", err)
	}
	return &pb.PutResponse{}, nil
}

func (s *KVServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	if s.raftNode != nil {
		err := s.raftNode.Propose(ctx, raft.Command{Op: raft.CmdDelete, Key: req.Key})
		if err != nil {
			var nle *raft.NotLeaderError
			if errors.As(err, &nle) {
				return nil, status.Errorf(codes.FailedPrecondition, "%v", nle)
			}
			return nil, status.Errorf(codes.Internal, "raft delete: %v", err)
		}
		return &pb.DeleteResponse{}, nil
	}

	if err := s.tree.Delete(req.Key); err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	return &pb.DeleteResponse{}, nil
}
