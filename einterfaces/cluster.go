// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package einterfaces

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/mattermost/mattermost-server/v6/model"
	"github.com/mattermost/mattermost-server/v6/shared/mlog"
)

type ClusterMessageHandler func(msg *model.ClusterMessage)

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

type ClusterImpl struct {
	mux                    sync.RWMutex
	clusterMessageHandlers map[model.ClusterEvent][]ClusterMessageHandler
}

func NewClusterImpl() *ClusterImpl {
	clusterImpl := &ClusterImpl{
		clusterMessageHandlers: make(map[model.ClusterEvent][]ClusterMessageHandler),
	}
	return clusterImpl
}

func (c *ClusterImpl) StartInterNodeCommunication() {
	mlog.Error("StartInterNodeCommunication")
}

func (c *ClusterImpl) StopInterNodeCommunication() {
	mlog.Error("StopInterNodeCommunication")
}

func (c *ClusterImpl) RegisterClusterMessageHandler(event model.ClusterEvent, crm ClusterMessageHandler) {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.clusterMessageHandlers[event] = append(c.clusterMessageHandlers[event], crm)
	mlog.Error("RegisterClusterMessageHandler")
}

func (c *ClusterImpl) GetClusterId() string {
	mlog.Error("GetClusterId")
	return "0"
}

func (c *ClusterImpl) IsLeader() bool {
	mlog.Error("IsLeader")
	return true
}

func (c *ClusterImpl) HealthScore() int {
	mlog.Error("HealthScore")
	return 0
}

func (c *ClusterImpl) GetMyClusterInfo() *model.ClusterInfo {
	mlog.Error("GetMyClusterInfo")
	return &model.ClusterInfo{}
}

func (c *ClusterImpl) GetClusterInfos() []*model.ClusterInfo {
	mlog.Error("GetClusterInfos")
	return []*model.ClusterInfo{{}}
}

func (c *ClusterImpl) SendClusterMessage(msg *model.ClusterMessage) {
	b, _ := json.Marshal(*msg)
	mlog.Error(fmt.Sprintf("SendClusterMessage %s", string(b)))
}

func (c *ClusterImpl) SendClusterMessageToNode(nodeID string, msg *model.ClusterMessage) error {
	mlog.Error("SendClusterMessageToNode")
	return nil
}

func (c *ClusterImpl) NotifyMsg(buf []byte) {
	mlog.Error("NotifyMsg")
}

func (c *ClusterImpl) GetClusterStats() ([]*model.ClusterStats, *model.AppError) {
	mlog.Error("GetClusterStats")
	return []*model.ClusterStats{}, nil
}

func (c *ClusterImpl) GetLogs(page, perPage int) ([]string, *model.AppError) {
	mlog.Error("GetLogs")
	return []string{}, nil
}

func (c *ClusterImpl) QueryLogs(page, perPage int) (map[string][]string, *model.AppError) {
	mlog.Error("QueryLogs")
	return make(map[string][]string), nil
}

func (c *ClusterImpl) GetPluginStatuses() (model.PluginStatuses, *model.AppError) {
	mlog.Error("GetPluginStatuses")
	return model.PluginStatuses{}, nil
}

func (c *ClusterImpl) ConfigChanged(previousConfig *model.Config, newConfig *model.Config, sendToOtherServer bool) *model.AppError {
	mlog.Error("ConfigChanged")
	return nil
}
