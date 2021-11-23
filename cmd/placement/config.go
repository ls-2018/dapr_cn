// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package main

import (
	"flag"
	"strings"

	"github.com/dapr/kit/logger"

	"github.com/dapr/dapr/pkg/metrics"
	"github.com/dapr/dapr/pkg/placement/raft"
)

const (
	defaultCredentialsPath   = "/var/run/dapr/credentials"
	defaultHealthzPort       = 8080
	defaultPlacementPort     = 50005
	defaultReplicationFactor = 100
)

type config struct {
	// Raft 协议配置
	raftID           string
	raftPeerString   string
	raftPeers        []raft.PeerInfo
	raftInMemEnabled bool
	raftLogStorePath string

	// Placement server 配置
	placementPort int
	healthzPort   int
	certChainPath string
	tlsEnabled    bool

	replicationFactor int

	// 日志、指标 配置
	loggerOptions   logger.Options
	metricsExporter metrics.Exporter
}

func newConfig() *config {
	// Default configuration
	cfg := config{
		raftID:           "dapr-placement-0",
		raftPeerString:   "dapr-placement-0=127.0.0.1:8201",
		raftPeers:        []raft.PeerInfo{},
		raftInMemEnabled: true,
		raftLogStorePath: "",

		placementPort: defaultPlacementPort,
		healthzPort:   defaultHealthzPort,
		certChainPath: defaultCredentialsPath,
		tlsEnabled:    false,
	}

	flag.StringVar(&cfg.raftID, "id", cfg.raftID, "Placement server ID.")
	flag.StringVar(&cfg.raftPeerString, "initial-cluster", cfg.raftPeerString, "raft cluster peers")
	flag.BoolVar(&cfg.raftInMemEnabled, "inmem-store-enabled", cfg.raftInMemEnabled, "Enable in-memory log and snapshot store unless --raft-logstore-path is set")
	flag.StringVar(&cfg.raftLogStorePath, "raft-logstore-path", cfg.raftLogStorePath, "raft log store path.")
	flag.IntVar(&cfg.placementPort, "port", cfg.placementPort, "sets the gRPC port for the placement service")
	flag.IntVar(&cfg.healthzPort, "healthz-port", cfg.healthzPort, "sets the HTTP port for the healthz server")
	flag.StringVar(&cfg.certChainPath, "certchain", cfg.certChainPath, "Path to the credentials directory holding the cert chain")
	flag.BoolVar(&cfg.tlsEnabled, "tls-enabled", cfg.tlsEnabled, "Should TLS be enabled for the placement gRPC server")
	flag.IntVar(&cfg.replicationFactor, "replicationFactor", defaultReplicationFactor, "sets the replication factor for actor distribution on vnodes")

	cfg.loggerOptions = logger.DefaultOptions()
	cfg.loggerOptions.AttachCmdFlags(flag.StringVar, flag.BoolVar)

	cfg.metricsExporter = metrics.NewExporter(metrics.DefaultMetricNamespace)
	cfg.metricsExporter.Options().AttachCmdFlags(flag.StringVar, flag.BoolVar)

	flag.Parse()

	cfg.raftPeers = parsePeersFromFlag(cfg.raftPeerString)
	if cfg.raftLogStorePath != "" {
		cfg.raftInMemEnabled = false
	}

	return &cfg
}

func parsePeersFromFlag(val string) []raft.PeerInfo {
	peers := []raft.PeerInfo{}

	p := strings.Split(val, ",")
	for _, addr := range p {
		peer := strings.Split(addr, "=")
		if len(peer) != 2 {
			continue
		}

		peers = append(peers, raft.PeerInfo{
			ID:      strings.TrimSpace(peer[0]),
			Address: strings.TrimSpace(peer[1]),
		})
	}

	return peers
}
