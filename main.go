package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "kvstore/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

type RaftNode struct {
	pb.UnimplementedRaftServiceServer
	id          int
	term        int
	state       string
	lastContact time.Time
	// address string
	votedFor int
	peers    map[int]pb.RaftServiceClient // Maps NodeId -> gRPC client
	mu       sync.Mutex
}

func (s *RaftNode) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fmt.Printf("Node %d: Received VoteRequest from Node %d for Term %d (My Term: %d, VotedFor: %d)\n", s.id, req.CandidateId, req.Term, s.term, s.votedFor)

	// if candidate's term is less than current term, reject
	if req.Term < int32(s.term) {
		return &pb.VoteResponse{Term: int32(s.term), VoteGranted: false}, nil
	}

	// If candidate's term is newer, update term and reset my vote
	if req.Term > int32(s.term) {
		fmt.Printf("Node %d: Seeing higher term (%d), stepping down to Follower\n", s.id, req.Term)
		s.term = int(req.Term)
		s.state = Follower
		s.votedFor = -1
	}

	// If not voted yet(or already voted for this candidate), grant vote
	if s.votedFor == -1 || s.votedFor == int(req.CandidateId) {
		fmt.Printf("Node %d: Granting vote to Node %d for Term %d\n", s.id, req.CandidateId, req.Term)
		s.votedFor = int(req.CandidateId)
		s.lastContact = time.Now()
		return &pb.VoteResponse{Term: int32(s.term), VoteGranted: true}, nil
	}

	return &pb.VoteResponse{Term: int32(s.term), VoteGranted: false}, nil
}

func (s *RaftNode) AppendEntries(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If leader's term is less than current term, reject
	if req.Term < int32(s.term) {
		return &pb.AppendResponse{Term: int32(s.term), Success: false}, nil
	}

	// If leader's term is newer or equal, accept them as leader
	fmt.Printf("Node %d received Heartbeat from Leader %d\n", s.id, req.LeaderId)
	s.lastContact = time.Now()
	s.term = int(req.Term)
	s.state = Follower

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

func (s *RaftNode) sendHeartbeats() {
	s.mu.Lock()
	savedTerm := s.term
	s.mu.Unlock()

	for id, client := range s.peers {
		go func(id int, client pb.RaftServiceClient) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
			defer cancel()

			resp, err := client.AppendEntries(ctx, &pb.AppendRequest{
				Term:     int32(savedTerm),
				LeaderId: int32(s.id),
			})

			if err != nil {
				// Peer might be down
				log.Printf("Node %d: Could not reach Peer %d: %v \n", s.id, id, err)
				return
			}

			// If peer has higher term, step down to follower
			if resp.Term > int32(savedTerm) {
				s.mu.Lock()
				fmt.Printf("Node %d: Found higher term (%d), stepping down\n", s.id, resp.Term)
				s.term = int(resp.Term)
				s.state = Follower
				s.mu.Unlock()
			}
		}(id, client)
	}
}

func (s *RaftNode) startElection() {
	s.mu.Lock()
	s.term++ //Move to the next term
	s.state = Candidate
	s.votedFor = s.id //vote for self
	savedTerm := s.term
	s.mu.Unlock()

	fmt.Printf("Node %d: Starting election for Term %d\n", s.id, savedTerm)
	votesReceived := 1 // Vote for self

	// var wg sync.WaitGroup
	for id, client := range s.peers {
		// wg.Add(1)
		go func(id int, client pb.RaftServiceClient) {
			// defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
			defer cancel()

			fmt.Printf("Node %d: Sending RequestVote to Peer %d\n", s.id, id)
			resp, err := client.RequestVote(ctx, &pb.VoteRequest{
				Term:        int32(savedTerm),
				CandidateId: int32(s.id),
			})

			if err != nil {
				fmt.Printf("Node %d: Error calling Peer %d: %v\n", s.id, id, err)
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()

			if resp.VoteGranted {
				votesReceived++
				// Check if we have majority (Quorum) (Total nodes / 2) + 1
				if votesReceived > (len(s.peers)+1)/2 && s.state == Candidate {
					fmt.Printf("Node %d: Won election for Term %d!\n", s.id, savedTerm)
					s.state = Leader
					s.lastContact = time.Now()
				}
			} else if resp.Term > int32(savedTerm) {
				s.state = Follower
				s.term = int(resp.Term)
			}
		}(id, client)
	}
}

func (s *RaftNode) run() {
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(s.id)))

	for {
		s.mu.Lock()
		currState := s.state
		s.mu.Unlock()

		if currState == Leader {
			// Leaders send heartbeats frequently (every 150ms)
			// to stay ahead of the Followers' election timers.
			s.sendHeartbeats()
			time.Sleep(150 * time.Millisecond)
			continue
		}

		// Followers and Candidates use a long, randomized timeout
		// to prevent "Split Votes" where everyone tries to be leader at once.
		timeout := time.Duration(1000+r.Intn(2000)) * time.Millisecond
		time.Sleep(timeout)

		s.mu.Lock()
		if s.state != Leader {
			if time.Since(s.lastContact) > timeout {
				fmt.Printf("Node %d: No heartbeat for %v, transitioning to Candidate!\n", s.id, timeout)
				s.state = Candidate
				go s.startElection()
			}
		}
		s.mu.Unlock()
	}
}

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
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
		id:          *id,
		state:       Follower,
		votedFor:    -1,
		lastContact: time.Now(),
		peers:       make(map[int]pb.RaftServiceClient),
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServiceServer(grpcServer, node)

	go node.connectToPeers(peerMap)
	go node.run()
	// go node.startHeartbeatTicker()

	fmt.Printf("Node %d starting on port %s...\n", *id, *addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
