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
	return []string{}
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
	mux                    sync.RWMutex
	clusterMessageHandlers map[model.ClusterEvent][]ClusterMessageHandler
	// memberlist
	peers GetPeersInterface
	conf  *memberlist.Config
	list  *memberlist.Memberlist
}

func NewClusterImpl() *ClusterImpl {
	clusterImpl := &ClusterImpl{
		clusterMessageHandlers: make(map[model.ClusterEvent][]ClusterMessageHandler),
		peers:                  NewPeers(),
	}
	return clusterImpl
}

func (c *ClusterImpl) StartInterNodeCommunication() {
	mlog.Info("CLUSTER: StartInterNodeCommunication")

	c.conf = memberlist.DefaultLocalConfig()
	c.conf.Events = &EventDelegateImpl{}
	// conf.Name = "node1"

	list, err := memberlist.Create(c.conf)
	if err != nil {
		mlog.Info(fmt.Sprintf("CLUSTER: memberlist.Create error %s", err))
		panic("?")
	}

	c.list = list

	mlog.Info(fmt.Sprintf("CLUSTER: local node %s", nodeId(c.list.LocalNode())))

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
	mlog.Info("CLUSTER: IsLeader")
	return true
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

func (c *ClusterImpl) SendClusterMessage(msg *model.ClusterMessage) {
	mlog.Info("CLUSTER: SendClusterMessage")

	if c.list == nil {
		mlog.Info("CLUSTER: SendClusterMessage no cluster")
		return
	}

	bytes, err := json.Marshal(*msg)
	if err != nil {
		mlog.Info(fmt.Sprintf("CLUSTER: SendClusterMessage error %s", err))
		return
	}

	for _, node := range c.list.Members() {

		if msg.SendType == model.ClusterSendReliable {
			err = c.list.SendReliable(node, bytes)
		} else {
			err = c.list.SendBestEffort(node, bytes)
		}

		if err != nil {
			mlog.Info(fmt.Sprintf("CLUSTER: SendClusterMessage %s to %s error %s", string(bytes), nodeId(node), err))
		} else {
			mlog.Info(fmt.Sprintf("CLUSTER: SendClusterMessage %s to %s", string(bytes), nodeId(node)))
		}

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
		mlog.Info(fmt.Sprintf("CLUSTER: SendClusterMessage error %s", err))
		return err
	}

	for _, node := range c.list.Members() {

		if nodeId(node) != nodeID {
			continue
		}

		if msg.SendType == model.ClusterSendReliable {
			err = c.list.SendReliable(node, bytes)
		} else {
			err = c.list.SendBestEffort(node, bytes)
		}

		if err != nil {
			mlog.Info(fmt.Sprintf("CLUSTER: SendClusterMessage %s to %s error %s", string(bytes), nodeId(node), err))
			return err
		} else {
			mlog.Info(fmt.Sprintf("CLUSTER: SendClusterMessage %s to %s", string(bytes), nodeId(node)))
		}

	}
	return nil
}

func (c *ClusterImpl) NotifyMsg(buf []byte) {
	mlog.Info("CLUSTER: NotifyMsg")
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
