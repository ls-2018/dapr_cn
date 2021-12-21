// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package actors

import (
	"time"

	app_config "github.com/dapr/dapr/pkg/config"
)

// Config actor 运行时配置
type Config struct {
	HostAddress                   string
	AppID                         string
	PlacementAddresses            []string
	HostedActorTypes              []string
	Port                          int
	HeartbeatInterval             time.Duration
	ActorDeactivationScanInterval time.Duration
	ActorIdleTimeout              time.Duration
	DrainOngoingCallTimeout       time.Duration
	DrainRebalancedActors         bool
	Namespace                     string
	Reentrancy                    app_config.ReentrancyConfig
	RemindersStoragePartitions    int
}

const (
	defaultActorIdleTimeout     = time.Minute * 60 // 空闲时间
	defaultHeartbeatInterval    = time.Second * 1
	defaultActorScanInterval    = time.Second * 30 //
	defaultOngoingCallTimeout   = time.Second * 60
	defaultReentrancyStackLimit = 32
)

// NewConfig 返回一个 actor 运行时配置
func NewConfig(hostAddress, appID string, placementAddresses []string, hostedActors []string, port int,
	actorScanInterval, actorIdleTimeout, ongoingCallTimeout string, drainRebalancedActors bool, namespace string,
	reentrancy app_config.ReentrancyConfig, remindersStoragePartitions int) Config {
	c := Config{
		HostAddress:                   hostAddress,
		AppID:                         appID,
		PlacementAddresses:            placementAddresses,
		HostedActorTypes:              hostedActors,
		Port:                          port,
		HeartbeatInterval:             defaultHeartbeatInterval,
		ActorDeactivationScanInterval: defaultActorScanInterval, // 失活扫描触发器
		ActorIdleTimeout:              defaultActorIdleTimeout,
		DrainOngoingCallTimeout:       defaultOngoingCallTimeout,
		DrainRebalancedActors:         drainRebalancedActors,
		Namespace:                     namespace,
		Reentrancy:                    reentrancy,
		RemindersStoragePartitions:    remindersStoragePartitions,
	}

	scanDuration, err := time.ParseDuration(actorScanInterval)
	if err == nil {
		c.ActorDeactivationScanInterval = scanDuration
	}

	idleDuration, err := time.ParseDuration(actorIdleTimeout)
	if err == nil {
		c.ActorIdleTimeout = idleDuration
	}

	drainCallDuration, err := time.ParseDuration(ongoingCallTimeout)
	if err == nil {
		c.DrainOngoingCallTimeout = drainCallDuration
	}

	if reentrancy.MaxStackDepth == nil {
		reentrancyLimit := defaultReentrancyStackLimit
		c.Reentrancy.MaxStackDepth = &reentrancyLimit
	}

	return c
}
