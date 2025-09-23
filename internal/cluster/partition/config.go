// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package partition

import (
	"strings"
	"time"

	"github.com/pbinitiative/zenbpm/internal/config"
)

func GetRqLiteDefaultConfig(nodeId string, raftAddr string, dataPath string, joinAddresses []string) config.RqLite {
	return config.RqLite{
		DataPath:                    dataPath,
		AuthFile:                    "",
		AutoBackupFile:              "",
		AutoRestoreFile:             "",
		HTTPx509CACert:              "",
		HTTPx509Cert:                "",
		HTTPx509Key:                 "",
		HTTPVerifyClient:            false,
		NodeX509CACert:              "",
		NodeX509Cert:                "",
		NodeX509Key:                 "",
		NoNodeVerify:                true,
		NodeVerifyClient:            false,
		NodeVerifyServerName:        "",
		NodeID:                      nodeId,
		RaftAddr:                    raftAddr,
		RaftAdv:                     raftAddr,
		JoinAddrs:                   strings.Join(joinAddresses, ","),
		JoinAttempts:                5,
		JoinInterval:                3 * time.Second,
		JoinAs:                      "",
		BootstrapExpect:             1,
		BootstrapExpectTimeout:      120 * time.Second,
		OnDiskPath:                  "",
		FKConstraints:               true,
		AutoVacInterval:             12 * time.Hour,
		RaftLogLevel:                "WARN",
		RaftNonVoter:                false,
		RaftSnapThreshold:           8192,
		RaftSnapThresholdWALSize:    4 * 1024 * 1024,
		RaftSnapInterval:            10 * time.Second,
		RaftLeaderLeaseTimeout:      0,
		RaftHeartbeatTimeout:        1 * time.Second,
		RaftElectionTimeout:         1 * time.Second,
		RaftApplyTimeout:            5 * time.Second,
		RaftShutdownOnRemove:        false,
		RaftClusterRemoveOnShutdown: true,
		RaftStepdownOnShutdown:      true,
		RaftReapNodeTimeout:         0,
		RaftReapReadOnlyNodeTimeout: 0,
		ClusterConnectTimeout:       30 * time.Second,
		WriteQueueCap:               1024,
		WriteQueueBatchSz:           128,
		WriteQueueTimeout:           50 * time.Millisecond,
		WriteQueueTx:                false,
		CPUProfile:                  "",
		MemProfile:                  "",
	}
}
