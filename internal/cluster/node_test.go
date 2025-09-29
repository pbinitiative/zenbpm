// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package cluster

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/jobmanager"
	"github.com/pbinitiative/zenbpm/internal/cluster/partition"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/stretchr/testify/assert"
)

type testcluster struct {
	nodes      []*ZenNode
	leader     *ZenNode
	followers  []*ZenNode
	partitions map[uint32]testpartition
}

type testpartition struct {
	id                 uint32
	leader             *ZenNode
	followers          []*ZenNode
	leaderPartition    *partition.ZenPartitionNode
	followersPartition []*partition.ZenPartitionNode
}

func TestMultipleNodesOnePartition(t *testing.T) {
	cluster, err := startTestCluster(t, 3)
	assert.NoError(t, err)
	assert.NotNil(t, cluster.leader)

	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		status := cluster.leader.GetStatus()
		assert.NotEmpty(t, status.Partitions)
		assert.Equal(t, len(status.Partitions), status.Config.DesiredPartitions)
		for _, partition := range status.Partitions {
			partitionInfo := status.Partitions[partition.Id]
			assert.NotEmpty(t, partitionInfo.LeaderId, "leader of a partition is present")
			partitionLeaderNode := status.Nodes[partitionInfo.LeaderId]
			assert.NotEmpty(t, partitionLeaderNode, "leader node of a partition is present")
		}
	}, 5*time.Second, 500*time.Millisecond, "Cluster has information about partitions")

	partitionInitialized := assert.EventuallyWithT(t, func(t *assert.CollectT) {
		cluster.updatePartitions()
		partition := cluster.partitions[1]
		if !assert.NotEmpty(t, partition) {
			return
		}
		if !assert.NotNil(t, partition.leader) {
			fmt.Printf("%+v\n", partition)
			return
		}
		if !assert.True(t, partition.isLeaderInStatus(state.NodePartitionStateInitialized)) {
			fmt.Printf("%+v\n", cluster.leader.GetStatus())
			return
		}
		if !assert.NotNil(t, partition.leader.JobManager) {
			return
		}
		if !assert.NotNil(t, partition.leader.JobManager.Server) {
			return
		}
	}, 10*time.Second, 500*time.Millisecond, "Cluster has information about partitions")
	if !partitionInitialized {
		t.Fatal("partition was not initialized")
	}

	t.Run("Grpc client can connect to leader", func(t *testing.T) {
		jobCh := make(chan jobmanager.Job)
		assert.NoError(t, err)
		clientId := jobmanager.ClientID("test-client-c41261b0-d6dd-4d11-b259-f8b2287f94cb")
		jobType := jobmanager.JobType("job-type-d8ef656b-47e0-4908-9ca8-6e691ab494f1")
		partitionLeader := cluster.partitions[1].leader
		partitionLeader.JobManager.AddClient(t.Context(), clientId, jobCh)
		partitionLeader.JobManager.AddClientJobSub(t.Context(), clientId, jobType)
		serverJobTypes := cluster.leader.JobManager.Server.GetJobTypes()
		fmt.Printf("%+v\n", serverJobTypes)
		assert.EventuallyWithT(t, func(t *assert.CollectT) {
			assert.Equal(t, clientId, serverJobTypes[jobType].Clients[0])
		}, 5*time.Second, 100*time.Second, "client is connected to job manager")
	})
}

func startTestCluster(t *testing.T, nodeCount int) (testcluster, error) {
	cluster := testcluster{
		nodes:     make([]*ZenNode, nodeCount),
		followers: make([]*ZenNode, 0, nodeCount-1),
	}
	wg := sync.WaitGroup{}

	var joinErr error
	for i := range nodeCount {
		wg.Add(1)
		go func() {
			var err error
			conf := nodeConfig(t, i, nodeCount)
			cluster.nodes[i], err = StartZenNode(t.Context(), conf)
			if err != nil {
				joinErr = errors.Join(joinErr, err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if joinErr != nil {
		return cluster, fmt.Errorf("Failed to start nodes: %w", joinErr)
	}

	for _, node := range cluster.nodes {
		node.store.WaitForLeader(5 * time.Second)
		if node == nil {
			return cluster, fmt.Errorf("Failed to initialize %s", node.store.ID())
		}
		if node.store.IsLeader() {
			cluster.leader = node
		} else {
			cluster.followers = append(cluster.followers, node)
		}
	}
	cluster.updatePartitions()
	return cluster, nil
}

func (c *testcluster) updatePartitions() {
	status := c.leader.GetStatus()
	c.partitions = map[uint32]testpartition{}

	for _, node := range status.Nodes {
		for _, part := range node.Partitions {
			if _, ok := c.partitions[part.Id]; !ok {
				c.partitions[part.Id] = testpartition{
					id:                 part.Id,
					followersPartition: []*partition.ZenPartitionNode{},
				}
			}
			testPartition := c.partitions[part.Id]
			clusterNode := c.getNode(node.Id)
			if part.Role == state.RoleLeader {
				testPartition.leader = clusterNode
				testPartition.leaderPartition = clusterNode.controller.GetPartition(context.Background(), part.Id)
			} else {
				testPartition.followers = append(testPartition.followers, clusterNode)
				testPartition.followersPartition = append(testPartition.followersPartition, clusterNode.controller.GetPartition(context.Background(), part.Id))
			}
			c.partitions[part.Id] = testPartition
		}
	}
}

func (c *testcluster) getNode(id string) *ZenNode {
	for _, node := range c.nodes {
		if node.store.ID() == id {
			return node
		}
	}
	return nil
}

func nodeConfig(t *testing.T, nodeNumber int, numberOfNodes int) config.Config {
	joinAddresses := make([]string, numberOfNodes)
	for i := range joinAddresses {
		joinAddresses[i] = fmt.Sprintf("localhost:15%03d", i)
	}
	return config.Config{
		HttpServer: config.HttpServer{
			Addr: fmt.Sprintf(":13%03d", nodeNumber),
		},
		GrpcServer: config.GrpcServer{
			Addr: fmt.Sprintf(":14%03d", nodeNumber),
		},
		Cluster: config.Cluster{
			NodeId: fmt.Sprintf("node%d", nodeNumber),
			Addr:   fmt.Sprintf("localhost:15%03d", nodeNumber),
			Adv:    fmt.Sprintf("localhost:15%03d", nodeNumber),
			Raft: config.ClusterRaft{
				Dir:                    filepath.Join(t.TempDir(), fmt.Sprintf("node%d", nodeNumber)),
				JoinAttempts:           5,
				JoinInterval:           1 * time.Second,
				BootstrapExpect:        numberOfNodes,
				BootstrapExpectTimeout: 10 * time.Second,
				JoinAddresses:          joinAddresses,
			},
		},
	}
}

func (p *testpartition) isLeaderInStatus(state state.NodePartitionState) bool {
	status := p.leader.GetStatus()
	return status.Nodes[p.leader.store.ID()].Partitions[p.id].State == state
}
