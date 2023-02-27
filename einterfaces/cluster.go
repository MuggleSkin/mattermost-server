// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package einterfaces

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/mattermost/mattermost-server/v6/model"
	"github.com/mattermost/mattermost-server/v6/shared/mlog"
)

var _ memberlist.EventDelegate = (*EventDelegateImpl)(nil)
var _ memberlist.Delegate = (*ClusterImpl)(nil)
var _ ClusterInterface = (*ClusterImpl)(nil)

func nodeId(node *memberlist.Node) string {
	node.FullAddress()
	return fmt.Sprintf("%s (%s:%d)", node.Name, node.Addr.String(), node.Port)
}

func nodeInfo(node *memberlist.Node, protocol uint8) *model.ClusterInfo {
	hostname, _ := net.LookupAddr(node.Addr.String())
	return &model.ClusterInfo{
		Id:         nodeId(node),
		Version:    fmt.Sprintf("%v", protocol),
		ConfigHash: "0", // TODO
		IPAddress:  node.Addr.String(),
		Hostname:   strings.Join(hostname, "."),
	}
}

type ClusterMessageHandler func(msg *model.ClusterMessage)

type GetPeersInterface interface {
	GetPeers() []string
}

type GetPeersImpl struct {
}

func (peers *GetPeersImpl) GetPeers() []string {
	return []string{"0.0.0.0:7950"}
}

func NewPeers() *GetPeersImpl {
	return &GetPeersImpl{}
}

type ClusterInterface interface {
	StartInterNodeCommunication()
	StopInterNodeCommunication()
	RegisterClusterMessageHandler(event model.ClusterEvent, crm ClusterMessageHandler)
	GetClusterId() string
	IsLeader() bool
	// HealthScore returns a number which is indicative of how well an instance is meeting
	// the soft real-time requirements of the protocol. Lower numbers are better,
	// and zero means "totally healthy".
	HealthScore() int
	GetMyClusterInfo() *model.ClusterInfo
	GetClusterInfos() []*model.ClusterInfo
	SendClusterMessage(msg *model.ClusterMessage)
	SendClusterMessageToNode(nodeID string, msg *model.ClusterMessage) error
	NotifyMsg(buf []byte)
	GetClusterStats() ([]*model.ClusterStats, *model.AppError)
	GetLogs(page, perPage int) ([]string, *model.AppError)
	QueryLogs(page, perPage int) (map[string][]string, *model.AppError)
	GetPluginStatuses() (model.PluginStatuses, *model.AppError)
	ConfigChanged(previousConfig *model.Config, newConfig *model.Config, sendToOtherServer bool) *model.AppError
}

type EventDelegateImpl struct {
}

func (d *EventDelegateImpl) NotifyJoin(node *memberlist.Node) {
	mlog.Info(fmt.Sprintf("CLUSTER: NotifyJoin %s", nodeId(node)))
}

func (d *EventDelegateImpl) NotifyLeave(node *memberlist.Node) {
	mlog.Info(fmt.Sprintf("CLUSTER: NotifyLeave %s", nodeId(node)))
}

func (d *EventDelegateImpl) NotifyUpdate(node *memberlist.Node) {
	mlog.Info(fmt.Sprintf("CLUSTER: NotifyUpdate %s", nodeId(node)))
}

type ClusterImpl struct {
	config                 *model.Config
	mux                    sync.RWMutex
	clusterMessageHandlers map[model.ClusterEvent][]ClusterMessageHandler
	// memberlist
	peers GetPeersInterface
	conf  *memberlist.Config
	list  *memberlist.Memberlist
}

func (c *ClusterImpl) NotifyMsg(msg []byte) {
	mlog.Info("CLUSTER: NotifyMsg")

	c.mux.RLock()
	defer c.mux.RUnlock()

	clusterMsg := model.ClusterMessage{}
	err := json.Unmarshal([]byte(msg), &clusterMsg)
	if err != nil {
		mlog.Info(fmt.Sprintf("CLUSTER: NotifyMsg json.Unmarshal error %s", err))
		return
	}

	mlog.Info(fmt.Sprintf("CLUSTER: NotifyMsg %s", string(msg)))

	for _, handler := range c.clusterMessageHandlers[clusterMsg.Event] {
		go func(h ClusterMessageHandler) {
			h(&clusterMsg)
		}(handler)
	}
}

func (c *ClusterImpl) NodeMeta(limit int) []byte {
	mlog.Info("CLUSTER: NodeMeta")
	return []byte("")
}

func (c *ClusterImpl) LocalState(join bool) []byte {
	mlog.Info("CLUSTER: LocalState")
	return []byte("")
}

func (c *ClusterImpl) GetBroadcasts(overhead, limit int) [][]byte {
	mlog.Info("CLUSTER: GetBroadcasts")
	return nil
}

func (c *ClusterImpl) MergeRemoteState(buf []byte, join bool) {
	mlog.Info("CLUSTER: MergeRemoteState")
}

func NewClusterImpl(config *model.Config) *ClusterImpl {
	clusterImpl := &ClusterImpl{
		config:                 config,
		clusterMessageHandlers: make(map[model.ClusterEvent][]ClusterMessageHandler),
		peers:                  NewPeers(),
	}
	return clusterImpl
}

func (c *ClusterImpl) StartInterNodeCommunication() {
	mlog.Info("CLUSTER: StartInterNodeCommunication")

	c.conf = memberlist.DefaultLocalConfig()
	c.conf.Delegate = c
	c.conf.Events = &EventDelegateImpl{}

	if *c.config.ClusterSettings.OverrideHostname != "" {
		c.conf.Name = *c.config.ClusterSettings.OverrideHostname
	}

	c.conf.BindPort = *c.config.ClusterSettings.GossipPort
	c.conf.AdvertisePort = c.conf.BindPort

	c.conf.EnableCompression = *c.config.ClusterSettings.EnableGossipCompression

	list, err := memberlist.Create(c.conf)
	if err != nil {
		mlog.Info(fmt.Sprintf("CLUSTER: memberlist.Create error %s", err))
		panic("?")
	}

	c.list = list

	mlog.Info(fmt.Sprintf("CLUSTER: local node %s", nodeId(c.list.LocalNode())))

	peers := c.peers.GetPeers()
	if peers == nil {
		mlog.Info("CLUSTER: no peers")
		return
	}

	if reached, err := list.Join(c.peers.GetPeers()); err != nil {
		if err != nil {
			mlog.Info(fmt.Sprintf("CLUSTER: joined cluster %d", reached))
		} else {
			mlog.Info(fmt.Sprintf("CLUSTER: can't join cluster %s", err))
		}
	}
}

func (c *ClusterImpl) StopInterNodeCommunication() {
	mlog.Info("CLUSTER: StopInterNodeCommunication")

	if c.list == nil {
		mlog.Info("CLUSTER: StopInterNodeCommunication no cluster")
		return
	}

	const leaveTimeoutSeconds = 60

	err := c.list.Leave(time.Duration(leaveTimeoutSeconds) * time.Second)
	if err != nil {
		mlog.Info(fmt.Sprintf("CLUSTER: Leave error %s", err))
	}

	err = c.list.Shutdown()
	if err != nil {
		mlog.Info(fmt.Sprintf("CLUSTER: Shutdown error %s", err))
	}
}

func (c *ClusterImpl) RegisterClusterMessageHandler(event model.ClusterEvent, crm ClusterMessageHandler) {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.clusterMessageHandlers[event] = append(c.clusterMessageHandlers[event], crm)
	mlog.Info("CLUSTER: RegisterClusterMessageHandler")
}

func (c *ClusterImpl) GetClusterId() string {
	mlog.Info("CLUSTER: GetClusterId")
	if c.list == nil {
		mlog.Info("CLUSTER: GetClusterId no cluster")
		return ""
	}
	return nodeId(c.list.LocalNode())
}

func (c *ClusterImpl) IsLeader() bool {
	// https://en.wikipedia.org/wiki/Bully_algorithm
	mlog.Info("CLUSTER: IsLeader")

	var maxNode *string

	for _, node := range c.list.Members() {
		curNode := nodeId(node)
		if maxNode == nil || *maxNode < curNode {
			maxNode = &curNode
		}
	}

	return c.GetClusterId() == *maxNode
}

func (c *ClusterImpl) HealthScore() int {
	if c.list == nil {
		mlog.Info("CLUSTER: HealthScore no cluster")
		return math.MaxInt
	}
	healthScore := c.list.GetHealthScore()
	mlog.Info(fmt.Sprintf("CLUSTER: HealthScore %d", healthScore))
	return healthScore
}

func (c *ClusterImpl) GetMyClusterInfo() *model.ClusterInfo {
	mlog.Info("CLUSTER: GetMyClusterInfo")

	if c.list == nil {
		mlog.Info("CLUSTER: GetMyClusterInfo no cluster")
		return &model.ClusterInfo{}
	}

	return nodeInfo(c.list.LocalNode(), c.list.ProtocolVersion())
}

func (c *ClusterImpl) GetClusterInfos() []*model.ClusterInfo {
	mlog.Info("CLUSTER: GetClusterInfos")

	if c.list == nil {
		mlog.Info("CLUSTER: GetClusterInfos no cluster")
		return []*model.ClusterInfo{}
	}

	cluster := []*model.ClusterInfo{}

	for _, node := range c.list.Members() {
		cluster = append(cluster, nodeInfo(node, c.list.ProtocolVersion()))
	}

	return cluster
}

func (c *ClusterImpl) sendMessage(node *memberlist.Node, sendType string, bytes []byte) {
	var err error = nil

	if sendType == model.ClusterSendReliable {
		err = c.list.SendReliable(node, bytes)
	} else {
		err = c.list.SendBestEffort(node, bytes)
	}

	if err != nil {
		mlog.Info(fmt.Sprintf("CLUSTER: sendMessage %s to %s error %s", string(bytes), nodeId(node), err))
	} else {
		mlog.Info(fmt.Sprintf("CLUSTER: sendMessage %s to %s", string(bytes), nodeId(node)))
	}
}

func (c *ClusterImpl) SendClusterMessage(msg *model.ClusterMessage) {
	mlog.Info("CLUSTER: SendClusterMessage")

	if c.list == nil {
		mlog.Info("CLUSTER: SendClusterMessage no cluster")
		return
	}

	bytes, err := json.Marshal(*msg)
	if err != nil {
		mlog.Info(fmt.Sprintf("CLUSTER: SendClusterMessage json.Marshal error %s", err))
		return
	}

	for _, node := range c.list.Members() {
		if c.GetClusterId() == nodeId(node) {
			continue
		}
		go func(n *memberlist.Node) {
			c.sendMessage(n, msg.SendType, bytes)
		}(node)
	}
}

func (c *ClusterImpl) SendClusterMessageToNode(nodeID string, msg *model.ClusterMessage) error {
	mlog.Info("CLUSTER: SendClusterMessageToNode")

	if c.list == nil {
		mlog.Info("CLUSTER: SendClusterMessageToNode no cluster")
		return nil
	}

	bytes, err := json.Marshal(*msg)
	if err != nil {
		mlog.Info(fmt.Sprintf("CLUSTER: SendClusterMessageToNode json.Marshal error %s", err))
		return err
	}

	for _, node := range c.list.Members() {
		if nodeId(node) != nodeID {
			continue
		}
		go func(n *memberlist.Node) {
			c.sendMessage(n, msg.SendType, bytes)
		}(node)
	}
	return nil
}

func (c *ClusterImpl) GetClusterStats() ([]*model.ClusterStats, *model.AppError) {
	mlog.Info("CLUSTER: GetClusterStats")
	return []*model.ClusterStats{}, nil
}

func (c *ClusterImpl) GetLogs(page, perPage int) ([]string, *model.AppError) {
	mlog.Info("CLUSTER: GetLogs")
	return []string{}, nil
}

func (c *ClusterImpl) QueryLogs(page, perPage int) (map[string][]string, *model.AppError) {
	mlog.Info("CLUSTER: QueryLogs")
	return make(map[string][]string), nil
}

func (c *ClusterImpl) GetPluginStatuses() (model.PluginStatuses, *model.AppError) {
	mlog.Info("CLUSTER: GetPluginStatuses")
	return model.PluginStatuses{}, nil
}

func (c *ClusterImpl) ConfigChanged(previousConfig *model.Config, newConfig *model.Config, sendToOtherServer bool) *model.AppError {
	mlog.Info("CLUSTER: ConfigChanged")
	return nil
}
