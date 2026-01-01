package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "kvstore/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RaftNode struct {
	pb.UnimplementedRaftServiceServer
	id int
	term int
	address string
	peers map[int]pb.RaftServiceClient // Maps NodeId -> gRPC client
	mu sync.Mutex
}

func (s *RaftNode) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	fmt.Printf("Node %d received Vote Request from Node %d\n", s.id, req.CandidateId)
	return &pb.VoteResponse{Term: int32(s.term), VoteGranted: true}, nil
}

func (s *RaftNode) AppendEntries(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	fmt.Printf("Node %d received Heartbeat from Leader %d\n", s.id, req.LeaderId)
	return &pb.AppendResponse{Term: int32(s.term), Success: true}, nil
}

func (s *RaftNode) connectToPeers(peerAddrs map[int]string) {
	for id, addr := range peerAddrs {
		if id == s.id {
			continue
		}

		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Could not connect to peer %d at %s: %v", id, addr, err)
			continue
		}

		s.mu.Lock()
		s.peers[id] = pb.NewRaftServiceClient(conn)
		s.mu.Unlock()
		fmt.Printf("Node %d connected to Peer %d\n", s.id, id)
	}
}

func (s *RaftNode) startHeartbeatTicker() {
	ticker := time.NewTicker(2 * time.Second)
	for range ticker.C {
		s.mu.Lock()
		for id, client := range s.peers {
			go func(id int, client pb.RaftServiceClient) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
				defer cancel()

				_, err := client.AppendEntries(ctx, &pb.AppendRequest{
					Term: int32(s.term),
					LeaderId: int32(s.id),
				})

				if err != nil {
					// log.Printf("Failed to send heartbeat to %d: %v", id, err)
				} else {
					fmt.Printf("Sent heartbeat to %d\n", id)
				}
			}(id, client)
		}
		s.mu.Unlock()
	}
}

func main() {

	id := flag.Int("id", 1, "Node ID")
	addr := flag.String("addr", "localhost:50051", "Node Address")
	peersFlag := flag.String("peers", "", "Comma-Sperated list of id:addr")
	flag.Parse()

	peerMap := make(map[int]string)
	if *peersFlag != "" {
		parts := strings.Split(*peersFlag, ",")
		for _, p := range parts {
			kv := strings.Split(p, ":")
			pId, _ := strconv.Atoi(kv[0])
			pAddr := kv[1] + ":" + kv[2]
			peerMap[pId] = pAddr
		}

	}

	node := &RaftNode{
		id: *id,
		peers: make(map[int]pb.RaftServiceClient),
	}
	
	lis, err:= net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServiceServer(grpcServer, node)

	go node.connectToPeers(peerMap)
	go node.startHeartbeatTicker()

	fmt.Printf("Node %d starting on port %s...\n", *id, *addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
