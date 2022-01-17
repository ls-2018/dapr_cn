// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package placement

import (
	"sync"
	"time"

	"google.golang.org/grpc/peer"

	"github.com/dapr/dapr/pkg/placement/monitoring"
	"github.com/dapr/dapr/pkg/placement/raft"
	v1pb "github.com/dapr/dapr/pkg/proto/placement/v1"

	"github.com/dapr/kit/retry"
)

const (
	// raftApplyCommandMaxConcurrency is the max concurrency to apply command log to raft.
	raftApplyCommandMaxConcurrency = 10
	barrierWriteTimeout            = 2 * time.Minute
)

// MonitorLeadership 	// 监听leader变化,借鉴的consul的代码
//是用来监督我们是否获得或失去我们的角色 作为Raft集群的领导者。领导者有一些工作要做
//因此，我们必须对变化作出反应
//
// reference: https://github.com/hashicorp/consul/blob/master/agent/consul/leader.go
func (p *Service) MonitorLeadership() {
	var weAreLeaderCh chan struct{}
	var leaderLoop sync.WaitGroup

	leaderCh := p.raftNode.Raft().LeaderCh()

	for {
		select {
		case isLeader := <-leaderCh:
			if isLeader {
				// 可能在本身就是leader的情况下
				if weAreLeaderCh != nil {
					log.Error("试图在运行过程中启动leader loop")
					continue
				}

				weAreLeaderCh = make(chan struct{})
				leaderLoop.Add(1)
				go func(ch chan struct{}) {
					defer leaderLoop.Done()
					p.leaderLoop(ch)
				}(weAreLeaderCh)
				log.Info("获得的集群领导权")
			} else {
				if weAreLeaderCh == nil {
					log.Error("试图在不运行的情况下停止leader loop")
					continue
				}

				log.Info("停止 leader loop")
				close(weAreLeaderCh)
				leaderLoop.Wait() // 确保上边的go routinue 正常结束了
				weAreLeaderCh = nil
				log.Info("丢失集群领导权")
			}

		case <-p.shutdownCh:
			return
		}
	}
}

func (p *Service) leaderLoop(stopCh chan struct{}) {
	// 这个循环是为了确保FSM通过应用屏障来反映所有排队的写入，并在成为领导之前完成领导的建立。
	for !p.hasLeadership.Load() {
		// 如果不是 leader
		// for earlier stop
		select {
		case <-stopCh:
			return
		case <-p.shutdownCh:
			return
		default:
		}
		//是用来发布一个命令，该命令会阻断直到所有前面的操作都被应用到FSM上。
		//它可以用来确保FSM反映所有排队的写操作。可以提供一个可选的超时来限制我们等待命令启动的时间。
		//这必须在领导者上运行，否则会失败。
		barrier := p.raftNode.Raft().Barrier(barrierWriteTimeout)
		if err := barrier.Error(); err != nil {
			log.Error("没能等到屏障", "error", err)
			continue
		}

		if !p.hasLeadership.Load() {
			p.establishLeadership()
			log.Info("获得 leader.")
			// 撤销leader 的过程必须在leaderLoop()结束前完成。
			defer p.revokeLeadership()
		}
	}

	p.membershipChangeWorker(stopCh)
}

// 确立leader角色
func (p *Service) establishLeadership() {
	// Give more time to let each runtime to find the leader and connect to the leader.
	// 给予更多的时间，让每个runtime找到leader并与leader连接。
	p.faultyHostDetectDuration.Store(int64(faultyHostDetectInitialDuration))

	p.membershipCh = make(chan hostMemberChange, membershipChangeChSize)
	p.hasLeadership.Store(true)
}

//  撤销领导者角色
func (p *Service) revokeLeadership() {
	p.hasLeadership.Store(false)

	log.Info("等到所有的连接都被耗尽")
	p.streamConnGroup.Wait()

	p.cleanupHeartbeats()
}

// 清理心跳
func (p *Service) cleanupHeartbeats() {
	p.lastHeartBeat.Range(func(key, value interface{}) bool {
		p.lastHeartBeat.Delete(key)
		return true
	})
}

// membershipChangeWorker  更新成员状态的哈希表
func (p *Service) membershipChangeWorker(stopCh chan struct{}) {
	faultyHostDetectTimer := time.NewTicker(faultyHostDetectInterval) // 故障节点 检测时间
	disseminateTimer := time.NewTicker(disseminateTimerInterval)// 广播最新的哈希表的时间

	p.memberUpdateCount.Store(0)

	go p.processRaftStateCommand(stopCh)

	for {
		select {
		case <-stopCh:
			faultyHostDetectTimer.Stop()
			disseminateTimer.Stop()
			return

		case <-p.shutdownCh:
			faultyHostDetectTimer.Stop()
			disseminateTimer.Stop()
			return

		case t := <-disseminateTimer.C:
			// Earlier stop when leadership is lost.
			if !p.hasLeadership.Load() {
				continue
			}

			// check if there is actor runtime member change.
			if p.disseminateNextTime.Load() <= t.UnixNano() && len(p.membershipCh) == 0 {
				if cnt := p.memberUpdateCount.Load(); cnt > 0 {
					log.Debugf("Add raft.TableDisseminate to membershipCh. memberUpdateCount count: %d", cnt)
					p.membershipCh <- hostMemberChange{cmdType: raft.TableDisseminate}
				}
			}

		case t := <-faultyHostDetectTimer.C:
			// Earlier stop when leadership is lost.
			if !p.hasLeadership.Load() {
				continue
			}

			// Each dapr runtime sends the heartbeat every one second and placement will update UpdatedAt timestamp.
			// If UpdatedAt is outdated, we can mark the host as faulty node.
			// This faulty host will be removed from membership in the next dissemination period.
			if len(p.membershipCh) == 0 {
				m := p.raftNode.FSM().State().Members()
				for _, v := range m {
					// Earlier stop when leadership is lost.
					if !p.hasLeadership.Load() {
						break
					}

					// When leader is changed and current placement node become new leader, there are no stream
					// connections from each runtime so that lastHeartBeat have no heartbeat timestamp record
					// from each runtime. Eventually, all runtime will find the leader and connect to the leader
					// of placement servers.
					// Before all runtimes connect to the leader of placements, it will record the current
					// time as heartbeat timestamp.
					heartbeat, _ := p.lastHeartBeat.LoadOrStore(v.Name, time.Now().UnixNano())

					elapsed := t.UnixNano() - heartbeat.(int64)
					if elapsed < p.faultyHostDetectDuration.Load() {
						continue
					}
					log.Debugf("Try to remove outdated host: %s, elapsed: %d ns", v.Name, elapsed)

					p.membershipCh <- hostMemberChange{
						cmdType: raft.MemberRemove,
						host:    raft.DaprHostMember{Name: v.Name},
					}
				}
			}
		}
	}
}

// processRaftStateCommand is the worker loop to apply membership change command to raft state
// and will disseminate the latest hashing table to the connected dapr runtime.
func (p *Service) processRaftStateCommand(stopCh chan struct{}) {
	// logApplyConcurrency is the buffered channel to limit the concurrency
	// of raft apply command.
	logApplyConcurrency := make(chan struct{}, raftApplyCommandMaxConcurrency)

	for {
		select {
		case <-stopCh:
			return

		case <-p.shutdownCh:
			return

		case op := <-p.membershipCh:
			switch op.cmdType {
			case raft.MemberUpsert, raft.MemberRemove:
				// MemberUpsert updates the state of dapr runtime host whenever
				// Dapr runtime sends heartbeats every 1 second.
				// MemberRemove will be queued by faultHostDetectTimer.
				// Even if ApplyCommand is failed, both commands will retry
				// until the state is consistent.
				logApplyConcurrency <- struct{}{}
				go func() {
					// We lock dissemination to ensure the updates can complete before the table is disseminated.
					p.disseminateLock.Lock()
					defer p.disseminateLock.Unlock()

					updated, raftErr := p.raftNode.ApplyCommand(op.cmdType, op.host)
					if raftErr != nil {
						log.Errorf("fail to apply command: %v", raftErr)
					} else {
						if op.cmdType == raft.MemberRemove {
							p.lastHeartBeat.Delete(op.host.Name)
						}

						// ApplyCommand returns true only if the command changes hashing table.
						if updated {
							p.memberUpdateCount.Inc()
							// disseminateNextTime will be updated whenever apply is done, so that
							// it will keep moving the time to disseminate the table, which will
							// reduce the unnecessary table dissemination.
							p.disseminateNextTime.Store(time.Now().Add(disseminateTimeout).UnixNano())
						}
					}
					<-logApplyConcurrency
				}()

			case raft.TableDisseminate:
				// TableDisseminate will be triggered by disseminateTimer.
				// This disseminates the latest consistent hashing tables to Dapr runtime.
				p.performTableDissemination()
			}
		}
	}
}

func (p *Service) performTableDissemination() {
	p.streamConnPoolLock.RLock()
	nStreamConnPool := len(p.streamConnPool)
	p.streamConnPoolLock.RUnlock()
	nTargetConns := len(p.raftNode.FSM().State().Members())

	monitoring.RecordRuntimesCount(nStreamConnPool)
	monitoring.RecordActorRuntimesCount(nTargetConns)

	// ignore dissemination if there is no member update.
	if cnt := p.memberUpdateCount.Load(); cnt > 0 {
		p.disseminateLock.Lock()
		defer p.disseminateLock.Unlock()

		state := p.raftNode.FSM().PlacementState()
		log.Infof(
			"Start disseminating tables. memberUpdateCount: %d, streams: %d, targets: %d, table generation: %s",
			cnt, nStreamConnPool, nTargetConns, state.Version)
		p.streamConnPoolLock.RLock()
		streamConnPool := make([]placementGRPCStream, len(p.streamConnPool))
		copy(streamConnPool, p.streamConnPool)
		p.streamConnPoolLock.RUnlock()
		p.performTablesUpdate(streamConnPool, state)
		log.Infof(
			"Completed dissemination. memberUpdateCount: %d, streams: %d, targets: %d, table generation: %s",
			cnt, nStreamConnPool, nTargetConns, state.Version)
		p.memberUpdateCount.Store(0)

		// set faultyHostDetectDuration to the default duration.
		p.faultyHostDetectDuration.Store(int64(faultyHostDetectDefaultDuration))
	}
}

// performTablesUpdate updates the connected dapr runtimes using a 3 stage commit.
// It first locks so no further dapr can be taken it. Once placement table is locked
// in runtime, it proceeds to update new table to Dapr runtimes and then unlock
// once all runtimes have been updated.
//使用3个阶段的提交更新所连接的dapr运行时。它首先锁定，所以不能再有任何dapr被占用。
//一旦安置表在运行时被锁定，它将继续更新新表到Dapr运行时，然后解锁 一旦所有的运行时都被更新了，再解锁。
func (p *Service) performTablesUpdate(hosts []placementGRPCStream, newTable *v1pb.PlacementTables) {
	// TODO: error from disseminationOperation needs to be handle properly.
	// Otherwise, each Dapr runtime will have inconsistent hashing table.
	p.disseminateOperation(hosts, "lock", nil)
	p.disseminateOperation(hosts, "update", newTable)
	p.disseminateOperation(hosts, "unlock", nil)
}

func (p *Service) disseminateOperation(targets []placementGRPCStream, operation string, tables *v1pb.PlacementTables) error {
	o := &v1pb.PlacementOrder{
		Operation: operation,
		Tables:    tables,
	}

	var err error
	for _, s := range targets {
		config := retry.DefaultConfig()
		config.MaxRetries = 3
		backoff := config.NewBackOff()

		retry.NotifyRecover(
			func() error {
				err = s.Send(o)

				if err != nil {
					remoteAddr := "n/a"
					if peer, ok := peer.FromContext(s.Context()); ok {
						remoteAddr = peer.Addr.String()
					}

					log.Errorf("error updating runtime host (%q) on %q operation: %s", remoteAddr, operation, err)
					return err
				}
				return nil
			},
			backoff,
			func(err error, d time.Duration) { log.Debugf("Attempting to disseminate again after error: %v", err) },
			func() { log.Debug("Dissemination successful.") })
	}

	return err
}
