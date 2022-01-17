// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package raft

import (
	"io"
	"sync"

	"github.com/google/go-cmp/cmp"
	"github.com/hashicorp/go-msgpack/codec"

	"github.com/dapr/dapr/pkg/placement/hashing"
)

// DaprHostMember  宿主机成员。
type DaprHostMember struct {
	// Dapr 运行时主机
	Name  string
	AppID string
	//此Dapr运行时支持的Actor类型列表。
	Entities []string
	// UpdatedAt 成员上一次更新的时间
	UpdatedAt int64
}

type DaprHostMemberStateData struct {
	// Index raft 日志索引号
	Index uint64
	// Members 包括Dapr运行时主机。
	Members map[string]*DaprHostMember

	// TableGeneration  hashingTableMap 版本号
	TableGeneration uint64

	// hashingTableMap is the map for storing consistent hashing data
	// hashingTableMap是用d于存储每个Actor类型的一致哈希数据的映射。当日志条目被重播时将会生成。在对状态进行快照时，
	//不会保存该成员。相反，hashingTableMap将在快照恢复过程中恢复。
	hashingTableMap map[string]*hashing.Consistent
}

// DaprHostMemberState 存储Dapr运行时主机和一致的哈希表的状态。
type DaprHostMemberState struct {
	lock sync.RWMutex
	data DaprHostMemberStateData
}

func newDaprHostMemberState() *DaprHostMemberState {
	return &DaprHostMemberState{
		data: DaprHostMemberStateData{
			Index:           0,
			TableGeneration: 0,
			Members:         map[string]*DaprHostMember{},
			hashingTableMap: map[string]*hashing.Consistent{},
		},
	}
}

func (s *DaprHostMemberState) Index() uint64 {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.data.Index
}

func (s *DaprHostMemberState) Members() map[string]*DaprHostMember {
	s.lock.RLock()
	defer s.lock.RUnlock()

	members := make(map[string]*DaprHostMember)
	for k, v := range s.data.Members {
		members[k] = v
	}
	return members
}

func (s *DaprHostMemberState) TableGeneration() uint64 {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.data.TableGeneration
}

func (s *DaprHostMemberState) hashingTableMap() map[string]*hashing.Consistent {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.data.hashingTableMap
}

func (s *DaprHostMemberState) clone() *DaprHostMemberState {
	s.lock.RLock()
	defer s.lock.RUnlock()

	newMembers := &DaprHostMemberState{
		data: DaprHostMemberStateData{
			Index:           s.data.Index,
			TableGeneration: s.data.TableGeneration,
			Members:         map[string]*DaprHostMember{},
			hashingTableMap: nil,
		},
	}
	for k, v := range s.data.Members {
		m := &DaprHostMember{
			Name:      v.Name,
			AppID:     v.AppID,
			Entities:  make([]string, len(v.Entities)),
			UpdatedAt: v.UpdatedAt,
		}
		copy(m.Entities, v.Entities)
		newMembers.data.Members[k] = m
	}
	return newMembers
}

// caller should holds lock.
func (s *DaprHostMemberState) updateHashingTables(host *DaprHostMember) {
	for _, e := range host.Entities {
		if _, ok := s.data.hashingTableMap[e]; !ok {
			s.data.hashingTableMap[e] = hashing.NewConsistentHash()
		}

		s.data.hashingTableMap[e].Add(host.Name, host.AppID, 0)
	}
}

// caller should holds lock.
func (s *DaprHostMemberState) removeHashingTables(host *DaprHostMember) {
	for _, e := range host.Entities {
		if t, ok := s.data.hashingTableMap[e]; ok {
			t.Remove(host.Name)

			// if no dedicated actor service instance for the particular actor type,
			// we must delete consistent hashing table to avoid the memory leak.
			if len(t.Hosts()) == 0 {
				delete(s.data.hashingTableMap, e)
			}
		}
	}
}

// upsertMember upserts member host info to the FSM state and returns true
// if the hashing table update happens.
func (s *DaprHostMemberState) upsertMember(host *DaprHostMember) bool {
	if !s.isActorHost(host) {
		return false
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	if m, ok := s.data.Members[host.Name]; ok {
		// No need to update consistent hashing table if the same dapr host member exists
		if m.AppID == host.AppID && m.Name == host.Name && cmp.Equal(m.Entities, host.Entities) {
			m.UpdatedAt = host.UpdatedAt
			return false
		}

		// Remove hashing table because the existing member is invalid
		// and needs to be updated by new member info.
		s.removeHashingTables(m)
	}

	s.data.Members[host.Name] = &DaprHostMember{
		Name:      host.Name,
		AppID:     host.AppID,
		UpdatedAt: host.UpdatedAt,
	}

	// Update hashing table only when host reports actor types
	s.data.Members[host.Name].Entities = make([]string, len(host.Entities))
	copy(s.data.Members[host.Name].Entities, host.Entities)

	s.updateHashingTables(s.data.Members[host.Name])

	// Increase hashing table generation version. Runtime will compare the table generation
	// version with its own and then update it if it is new.
	s.data.TableGeneration++

	return true
}

// removeMember removes members from membership and update hashing table and returns true
// if hashing table update happens.
func (s *DaprHostMemberState) removeMember(host *DaprHostMember) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	if m, ok := s.data.Members[host.Name]; ok {
		s.removeHashingTables(m)
		s.data.TableGeneration++
		delete(s.data.Members, host.Name)

		return true
	}

	return false
}

func (s *DaprHostMemberState) isActorHost(host *DaprHostMember) bool {
	return len(host.Entities) > 0
}

// caller should holds lock.
func (s *DaprHostMemberState) restoreHashingTables() {
	if s.data.hashingTableMap == nil {
		s.data.hashingTableMap = map[string]*hashing.Consistent{}
	}

	for _, m := range s.data.Members {
		s.updateHashingTables(m)
	}
}

func (s *DaprHostMemberState) restore(r io.Reader) error {
	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})
	var data DaprHostMemberStateData
	if err := dec.Decode(&data); err != nil {
		return err
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	s.data = data

	s.restoreHashingTables()
	return nil
}

// 将s.data 写入到w
func (s *DaprHostMemberState) persist(w io.Writer) error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	// 序列化消息体
	b, err := marshalMsgPack(s.data)
	if err != nil {
		return err
	}

	if _, err := w.Write(b); err != nil {
		return err
	}

	return nil
}
