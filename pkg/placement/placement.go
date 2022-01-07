// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package placement

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/kit/logger"

	dapr_credentials "github.com/dapr/dapr/pkg/credentials"
	"github.com/dapr/dapr/pkg/placement/raft"
	placementv1pb "github.com/dapr/dapr/pkg/proto/placement/v1"
)

var log = logger.NewLogger("dapr.placement")

// 服务端流
type placementGRPCStream placementv1pb.Placement_ReportDaprStatusServer

const (
	// membershipChangeChSize 是来自Dapr runtime的成员变更请求的通道大小。MembershipChangeWorker将处理actor主持的成员变更请求。
	membershipChangeChSize = 100

	// faultyHostDetectDuration是现有主机被标记为故障的最长时间。Dapr运行时每1秒发送一次心跳。每当安置服务器得到心跳，
	//它就更新FSM状态UpdateAt中的最后一次心跳时间。如果Now - UpdatedAt超过faultyHostDetectDuration，
	//membershipChangeWorker() 会尝试将有问题的Dapr runtime从 会员资格。
	// 当放置得到领导权时，faultyHostDetectionDuration将是faultyHostDetectInitialDuration。
	//这个持续时间将给予更多的时间让每个运行时找到安置节点的领导。一旦在获得领导权后发生第一次传播， membershipChangeWorker将
	// use faultyHostDetectDefaultDuration.
	faultyHostDetectInitialDuration = 6 * time.Second
	faultyHostDetectDefaultDuration = 3 * time.Second

	// faultyHostDetectInterval 是检查故障成员的间隔时间。
	faultyHostDetectInterval = 500 * time.Millisecond

	// disseminateTimerInterval 是传播最新一致的散列表的时间间隔。
	disseminateTimerInterval = 500 * time.Millisecond
	// disseminateTimeout is the timeout to disseminate hashing tables after the membership change.
	// When the multiple actor service pods are deployed first, a few pods are deployed in the beginning
	// and the rest of pods will be deployed gradually. disseminateNextTime is maintained to decide when
	// the hashing table is disseminated. disseminateNextTime is updated whenever membership change
	// is applied to raft state or each pod is deployed. If we increase disseminateTimeout, it will
	// reduce the frequency of dissemination, but it will delay the table dissemination.
	disseminateTimeout = 2 * time.Second
)

type hostMemberChange struct {
	cmdType raft.CommandType
	host    raft.DaprHostMember
}

// Service updates the Dapr runtimes with distributed hash tables for stateful entities.
//更新Dapr runtime，为有状态实体提供分布式哈希表。
type Service struct {
	// serverListener placement grpc服务的tcp listener
	serverListener net.Listener
	// grpcServerLock
	grpcServerLock *sync.Mutex
	// grpcServer placement grpc服务
	grpcServer *grpc.Server
	// streamConnPool  placement gRPC server and Dapr runtime之间建立的流连接
	streamConnPool []placementGRPCStream
	// streamConnPoolLock streamConnPool操作的锁
	streamConnPoolLock *sync.RWMutex

	// raftNode raft服务的节点
	raftNode *raft.Server

	// lastHeartBeat
	lastHeartBeat *sync.Map
	// membershipCh 用于管理dapr runtime成员更新的channel
	membershipCh chan hostMemberChange
	// disseminateLock 是hash表传播的锁。
	disseminateLock *sync.Mutex
	// disseminateNextTime hash表传播的时间
	disseminateNextTime atomic.Int64
	// memberUpdateCount 表示有多少dapr运行时间需要改变。
	//一致的散列表。只有actor runtimes的心跳会增加这个。
	memberUpdateCount atomic.Uint32

	// faultyHostDetectDuration
	faultyHostDetectDuration *atomic.Int64 // 有故障的主机检测时间

	// hasLeadership 是否是leader
	hasLeadership atomic.Bool

	// streamConnGroup represents the number of stream connections.
	// This waits until all stream connections are drained when revoking leadership.
	//代表流连接的数量。
	//在撤销领导权时，这要等到所有的流连接被耗尽。
	streamConnGroup sync.WaitGroup

	// shutdownLock 停止锁
	shutdownLock *sync.Mutex
	// shutdownCh 优雅关闭的channel
	shutdownCh chan struct{}
}

// NewPlacementService 返回一个placement service
func NewPlacementService(raftNode *raft.Server) *Service {
	return &Service{
		disseminateLock:          &sync.Mutex{},
		streamConnPool:           []placementGRPCStream{},
		streamConnPoolLock:       &sync.RWMutex{},
		membershipCh:             make(chan hostMemberChange, membershipChangeChSize),
		faultyHostDetectDuration: atomic.NewInt64(int64(faultyHostDetectInitialDuration)),
		raftNode:                 raftNode,
		shutdownCh:               make(chan struct{}),
		grpcServerLock:           &sync.Mutex{},
		shutdownLock:             &sync.Mutex{},
		lastHeartBeat:            &sync.Map{},
	}
}

// Run 开启placement service gRPC server.
func (p *Service) Run(port string, certChain *dapr_credentials.CertChain) {
	var err error
	p.serverListener, err = net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	opts, err := dapr_credentials.GetServerOptions(certChain)
	if err != nil {
		log.Fatalf("error creating gRPC options: %s", err)
	}
	grpcServer := grpc.NewServer(opts...)
	placementv1pb.RegisterPlacementServer(grpcServer, p)
	p.grpcServerLock.Lock()
	p.grpcServer = grpcServer
	p.grpcServerLock.Unlock()

	if err := grpcServer.Serve(p.serverListener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// Shutdown 关闭所有服务端连接
func (p *Service) Shutdown() {
	p.shutdownLock.Lock()
	defer p.shutdownLock.Unlock()

	close(p.shutdownCh)

	// wait until hasLeadership is false by revokeLeadership()
	for p.hasLeadership.Load() {
		select {
		case <-time.After(5 * time.Second):
			goto TIMEOUT
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}

TIMEOUT:
	p.grpcServerLock.Lock()
	if p.grpcServer != nil {
		p.grpcServer.Stop()
		p.grpcServer = nil
	}
	p.grpcServerLock.Unlock()
	p.serverListener.Close()
}

// ReportDaprStatus 获得各个节点的状态
func (p *Service) ReportDaprStatus(stream placementv1pb.Placement_ReportDaprStatusServer) error {
	registeredMemberID := ""
	isActorRuntime := false

	p.streamConnGroup.Add(1)
	defer func() {
		p.streamConnGroup.Done()
		p.deleteStreamConn(stream)
	}()

	for p.hasLeadership.Load() {
		req, err := stream.Recv()
		switch err {
		case nil:
			if registeredMemberID == "" {
				registeredMemberID = req.Name
				p.addStreamConn(stream)
				// TODO: If each sidecar can report table version, then placement
				// doesn't need to disseminate tables to each sidecar.
				p.performTablesUpdate([]placementGRPCStream{stream}, p.raftNode.FSM().PlacementState())
				log.Debugf("Stream connection is established from %s", registeredMemberID)
			}

			// Ensure that the incoming runtime is actor instance.
			isActorRuntime = len(req.Entities) > 0
			if !isActorRuntime {
				// ignore if this runtime is non-actor.
				continue
			}

			// Record the heartbeat timestamp. This timestamp will be used to check if the member
			// state maintained by raft is valid or not. If the member is outdated based the timestamp
			// the member will be marked as faulty node and removed.
			p.lastHeartBeat.Store(req.Name, time.Now().UnixNano())

			members := p.raftNode.FSM().State().Members()

			// Upsert incoming member only if it is an actor service (not actor client) and
			// the existing member info is unmatched with the incoming member info.
			upsertRequired := true
			if m, ok := members[req.Name]; ok {
				if m.AppID == req.Id && m.Name == req.Name && cmp.Equal(m.Entities, req.Entities) {
					upsertRequired = false
				}
			}

			if upsertRequired {
				p.membershipCh <- hostMemberChange{
					cmdType: raft.MemberUpsert,
					host: raft.DaprHostMember{
						Name:      req.Name,
						AppID:     req.Id,
						Entities:  req.Entities,
						UpdatedAt: time.Now().UnixNano(),
					},
				}
			}

		default:
			if registeredMemberID == "" {
				log.Error("stream is disconnected before member is added")
				return nil
			}

			if err == io.EOF {
				log.Debugf("Stream connection is disconnected gracefully: %s", registeredMemberID)
				if isActorRuntime {
					p.membershipCh <- hostMemberChange{
						cmdType: raft.MemberRemove,
						host:    raft.DaprHostMember{Name: registeredMemberID},
					}
				}
			} else {
				// no actions for hashing table. Instead, MembershipChangeWorker will check
				// host updatedAt and if now - updatedAt > p.faultyHostDetectDuration, remove hosts.
				log.Debugf("Stream connection is disconnected with the error: %v", err)
			}

			return nil
		}
	}

	return status.Error(codes.FailedPrecondition, "only leader can serve the request")
}

// addStreamConn  dapr runtime <----> placement
func (p *Service) addStreamConn(conn placementGRPCStream) {
	p.streamConnPoolLock.Lock()
	p.streamConnPool = append(p.streamConnPool, conn)
	p.streamConnPoolLock.Unlock()
}

func (p *Service) deleteStreamConn(conn placementGRPCStream) {
	p.streamConnPoolLock.Lock()
	for i, c := range p.streamConnPool {
		if c == conn {
			p.streamConnPool = append(p.streamConnPool[:i], p.streamConnPool[i+1:]...)
			break
		}
	}
	p.streamConnPoolLock.Unlock()
}
