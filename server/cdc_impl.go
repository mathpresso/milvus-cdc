/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * //
 *     http://www.apache.org/licenses/LICENSE-2.0
 * //
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/pb"
	cdcreader "github.com/zilliztech/milvus-cdc/core/reader"
	"github.com/zilliztech/milvus-cdc/core/util"
	cdcwriter "github.com/zilliztech/milvus-cdc/core/writer"
	serverapi "github.com/zilliztech/milvus-cdc/server/api"
	servererror "github.com/zilliztech/milvus-cdc/server/error"
	"github.com/zilliztech/milvus-cdc/server/maintenance"
	"github.com/zilliztech/milvus-cdc/server/metrics"
	"github.com/zilliztech/milvus-cdc/server/model"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
	"github.com/zilliztech/milvus-cdc/server/model/request"
	"github.com/zilliztech/milvus-cdc/server/store"
)

type ReplicateEntity struct {
	channelManager api.ChannelManager
	targetClient   api.TargetAPI
	metaOp         api.MetaOp
	writerObj      api.Writer
	mqDispatcher   msgdispatcher.Client
	mqTTDispatcher msgdispatcher.Client
	entityQuitFunc func()
	taskQuitFuncs  *typeutil.ConcurrentMap[string, func()]
	refCnt         atomic.Int32
}

type MetaCDC struct {
	BaseCDC
	metaStoreFactory serverapi.MetaStoreFactory
	mqFactoryCreator cdcreader.FactoryCreator
	rootPath         string
	config           *CDCServerConfig

	// collectionNames are used to make sure no duplicate task for a collection.
	// key -> milvus ip:port, value -> collection names
	collectionNames struct {
		sync.RWMutex
		data        map[string][]string
		excludeData map[string][]string
	}
	cdcTasks struct {
		sync.RWMutex
		data map[string]*meta.TaskInfo
	}
	// factoryCreator FactoryCreator
	replicateEntityMap struct {
		sync.RWMutex
		data map[string]*ReplicateEntity
	}
}

func NewMetaCDC(serverConfig *CDCServerConfig) *MetaCDC {
	if serverConfig.MaxNameLength == 0 {
		serverConfig.MaxNameLength = 256
	}

	rootPath := serverConfig.MetaStoreConfig.RootPath
	var factory serverapi.MetaStoreFactory
	var err error
	switch serverConfig.MetaStoreConfig.StoreType {
	case "mysql":
		factory, err = store.NewMySQLMetaStore(context.Background(), serverConfig.MetaStoreConfig.MysqlSourceURL, rootPath)
		if err != nil {
			log.Panic("fail to new mysql meta store", zap.Error(err))
		}
	case "etcd":
		etcdServerConfig := GetEtcdServerConfigFromMetaConfig(serverConfig.MetaStoreConfig)
		factory, err = store.NewEtcdMetaStore(context.Background(), etcdServerConfig, rootPath)
		if err != nil {
			log.Panic("fail to new etcd meta store", zap.Error(err))
		}
	default:
		log.Panic("not support the meta store type, valid type: [mysql, etcd]", zap.String("type", serverConfig.MetaStoreConfig.StoreType))
	}

	if serverConfig.SourceConfig.ReplicateChan == "" {
		log.Panic("the replicate channel in the source config is empty")
	}

	_, err = util.GetEtcdClient(GetEtcdServerConfigFromSourceConfig(serverConfig.SourceConfig))
	if err != nil {
		log.Panic("fail to get etcd client for connect the source etcd data", zap.Error(err))
	}
	// TODO check mq status

	cdc := &MetaCDC{
		metaStoreFactory: factory,
		config:           serverConfig,
		mqFactoryCreator: cdcreader.NewDefaultFactoryCreator(),
	}
	cdc.collectionNames.data = make(map[string][]string)
	cdc.collectionNames.excludeData = make(map[string][]string)
	cdc.cdcTasks.data = make(map[string]*meta.TaskInfo)
	cdc.replicateEntityMap.data = make(map[string]*ReplicateEntity)
	return cdc
}

func (e *MetaCDC) ReloadTask() {
	ctx := context.Background()
	taskInfos, err := e.metaStoreFactory.GetTaskInfoMetaStore(ctx).Get(ctx, &meta.TaskInfo{}, nil)
	if err != nil {
		log.Panic("fail to get all task info", zap.Error(err))
	}

	for _, taskInfo := range taskInfos {
		milvusURI := GetMilvusURI(taskInfo.MilvusConnectParam)
		newCollectionNames := lo.Map(taskInfo.CollectionInfos, func(t model.CollectionInfo, _ int) string {
			return t.Name
		})
		e.collectionNames.data[milvusURI] = append(e.collectionNames.data[milvusURI], newCollectionNames...)
		e.collectionNames.excludeData[milvusURI] = append(e.collectionNames.excludeData[milvusURI], taskInfo.ExcludeCollections...)
		e.cdcTasks.Lock()
		e.cdcTasks.data[taskInfo.TaskID] = taskInfo
		e.cdcTasks.Unlock()

		metrics.TaskNumVec.Add(taskInfo.TaskID, taskInfo.State)
		metrics.TaskStateVec.WithLabelValues(taskInfo.TaskID).Set(float64(taskInfo.State))
		if err := e.startInternal(taskInfo, taskInfo.State == meta.TaskStateRunning); err != nil {
			log.Warn("fail to start the task", zap.Any("task_info", taskInfo), zap.Error(err))
			_ = e.pauseTaskWithReason(taskInfo.TaskID, "fail to start task, err: "+err.Error(), []meta.TaskState{})
		}
	}
}

func GetMilvusURI(milvusConnectParam model.MilvusConnectParam) string {
	if milvusConnectParam.URI != "" {
		return milvusConnectParam.URI
	}
	return util.GetURI(milvusConnectParam.Host, milvusConnectParam.Port, milvusConnectParam.EnableTLS)
}

func GetMilvusToken(milvusConnectParam model.MilvusConnectParam) string {
	if milvusConnectParam.Token != "" {
		return milvusConnectParam.Token
	}
	return util.GetToken(milvusConnectParam.Username, milvusConnectParam.Password)
}

func (e *MetaCDC) Create(req *request.CreateRequest) (resp *request.CreateResponse, err error) {
	defer func() {
		log.Info("create request done")
		if err != nil {
			log.Warn("fail to create cdc task", zap.Any("req", req), zap.Error(err))
		}
	}()

	log.Info("validating create request")
	if err = e.validCreateRequest(req); err != nil {
		return nil, err
	}

	log.Info("start to create cdc task")
	milvusURI := GetMilvusURI(req.MilvusConnectParam)
	newCollectionNames := lo.Map(req.CollectionInfos, func(t model.CollectionInfo, _ int) string {
		return t.Name
	})
	e.collectionNames.Lock()
	if names, ok := e.collectionNames.data[milvusURI]; ok {
		existAll := lo.Contains(names, cdcreader.AllCollection)
		duplicateCollections := lo.Filter(req.CollectionInfos, func(info model.CollectionInfo, _ int) bool {
			return (!existAll && lo.Contains(names, info.Name)) || (existAll && info.Name == cdcreader.AllCollection)
		})
		if len(duplicateCollections) > 0 {
			e.collectionNames.Unlock()
			return nil, servererror.NewClientError(fmt.Sprintf("some collections are duplicate with existing tasks, %v", lo.Map(duplicateCollections, func(t model.CollectionInfo, i int) string {
				return t.Name
			})))
		}
		if existAll {
			excludeCollectionNames := lo.Filter(e.collectionNames.excludeData[milvusURI], func(s string, _ int) bool {
				return !lo.Contains(names, s)
			})
			duplicateCollections = lo.Filter(req.CollectionInfos, func(info model.CollectionInfo, _ int) bool {
				return !lo.Contains(excludeCollectionNames, info.Name)
			})
			if len(duplicateCollections) > 0 {
				e.collectionNames.Unlock()
				return nil, servererror.NewClientError(fmt.Sprintf("some collections are duplicate with existing tasks, check the `*` collection task and other tasks, %v", lo.Map(duplicateCollections, func(t model.CollectionInfo, i int) string {
					return t.Name
				})))
			}
		}
	}
	log.Info("add new collection names to the collection names")
	// release lock early to accept other requests
	var excludeCollectionNames []string
	if newCollectionNames[0] == cdcreader.AllCollection {
		existCollectionNames := e.collectionNames.data[milvusURI]
		excludeCollectionNames = make([]string, len(existCollectionNames))
		copy(excludeCollectionNames, existCollectionNames)
		e.collectionNames.excludeData[milvusURI] = excludeCollectionNames
	}
	e.collectionNames.data[milvusURI] = append(e.collectionNames.data[milvusURI], newCollectionNames...)
	e.collectionNames.Unlock()

	revertCollectionNames := func() {
		e.collectionNames.Lock()
		defer e.collectionNames.Unlock()
		if newCollectionNames[0] == cdcreader.AllCollection {
			e.collectionNames.excludeData[milvusURI] = []string{}
		}
		e.collectionNames.data[milvusURI] = lo.Without(e.collectionNames.data[milvusURI], newCollectionNames...)
	}

	ctx := context.Background()
	getResp, err := e.metaStoreFactory.GetTaskInfoMetaStore(ctx).Get(ctx, &meta.TaskInfo{}, nil)
	if err != nil {
		return nil, servererror.NewServerError(errors.WithMessage(err, "fail to get task list to check num"))
	}
	if len(getResp) >= e.config.MaxTaskNum {
		return nil, servererror.NewServerError(errors.Newf("the task num has reach the limit, %d", e.config.MaxTaskNum))
	}

	log.Info("start to create task info")
	info := &meta.TaskInfo{
		TaskID:                e.getUUID(),
		MilvusConnectParam:    req.MilvusConnectParam,
		CollectionInfos:       req.CollectionInfos,
		RPCRequestChannelInfo: req.RPCChannelInfo,
		ExcludeCollections:    excludeCollectionNames,
		WriterCacheConfig:     req.BufferConfig,
		State:                 meta.TaskStateInitial,
	}
	for _, collectionInfo := range req.CollectionInfos {
		positions := make(map[string]*meta.PositionInfo, len(collectionInfo.Positions))
		collectionID := int64(-1)
		for vchannel, collectionPosition := range collectionInfo.Positions {
			channelInfo, err := util.ParseVChannel(vchannel)
			if err != nil {
				revertCollectionNames()
				return nil, servererror.NewClientError(fmt.Sprintf("the vchannel is invalid, %s, err: %s", vchannel, err.Error()))
			}
			decodePosition, err := util.Base64DecodeMsgPosition(collectionPosition)
			if err != nil {
				return nil, servererror.NewServerError(errors.WithMessage(err, "fail to decode the position data"))
			}
			p := &meta.PositionInfo{
				DataPair: &commonpb.KeyDataPair{
					Key:  channelInfo.PChannelName,
					Data: decodePosition.MsgID,
				},
			}
			positions[channelInfo.PChannelName] = p
			if collectionID == -1 {
				collectionID = channelInfo.CollectionID
			}
			if collectionID != channelInfo.CollectionID {
				revertCollectionNames()
				return nil, servererror.NewClientError("the channel position info should be in the same collection")
			}
		}
		collectionName := collectionInfo.Name
		metaPosition := &meta.TaskCollectionPosition{
			TaskID:         info.TaskID,
			CollectionID:   collectionID,
			CollectionName: collectionName,
			Positions:      positions,
		}
		err = e.metaStoreFactory.GetTaskCollectionPositionMetaStore(ctx).Put(ctx, metaPosition, nil)
		if err != nil {
			return nil, servererror.NewServerError(errors.WithMessage(err, "fail to put the task collection position to etcd"))
		}

		collectionInfo.Positions = make(map[string]string)
	}

	if req.RPCChannelInfo.Position != "" {
		decodePosition, err := util.Base64DecodeMsgPosition(req.RPCChannelInfo.Position)
		if err != nil {
			return nil, servererror.NewServerError(errors.WithMessage(err, "fail to decode the rpc position data"))
		}
		rpcChannel := e.getRPCChannelName(req.RPCChannelInfo)

		metaPosition := &meta.TaskCollectionPosition{
			TaskID:         info.TaskID,
			CollectionID:   model.ReplicateCollectionID,
			CollectionName: model.ReplicateCollectionName,
			Positions: map[string]*meta.PositionInfo{
				rpcChannel: {
					DataPair: &commonpb.KeyDataPair{
						Key:  rpcChannel,
						Data: decodePosition.MsgID,
					},
				},
			},
		}
		err = e.metaStoreFactory.GetTaskCollectionPositionMetaStore(ctx).Put(ctx, metaPosition, nil)
		if err != nil {
			return nil, servererror.NewServerError(errors.WithMessage(err, "fail to put the task rpc position to etcd"))
		}
		req.RPCChannelInfo.Position = ""
	}
	log.Info("start to put the task info to etcd")
	err = e.metaStoreFactory.GetTaskInfoMetaStore(ctx).Put(ctx, info, nil)
	if err != nil {
		revertCollectionNames()
		return nil, servererror.NewServerError(errors.WithMessage(err, "fail to put the task info to etcd"))
	}
	metrics.TaskNumVec.Add(info.TaskID, info.State)
	metrics.TaskStateVec.WithLabelValues(info.TaskID).Set(float64(info.State))
	e.cdcTasks.Lock()
	e.cdcTasks.data[info.TaskID] = info
	e.cdcTasks.Unlock()
	err = e.startInternal(info, false)
	if err != nil {
		deleteErr := e.delete(info.TaskID)
		if deleteErr != nil {
			log.Warn("fail to delete the task", zap.String("task_id", info.TaskID), zap.Error(deleteErr))
			return nil, servererror.NewServerError(deleteErr)
		}
		return nil, err
	}
	log.Info("create task done")
	return &request.CreateResponse{TaskID: info.TaskID}, nil
}

func (e *MetaCDC) getRPCChannelName(channelInfo model.ChannelInfo) string {
	if channelInfo.Name != "" {
		return channelInfo.Name
	}
	return e.config.SourceConfig.ReplicateChan
}

func (e *MetaCDC) validCreateRequest(req *request.CreateRequest) error {
	connectParam := req.MilvusConnectParam
	if connectParam.TargetDBType != "bigquery" {
		if connectParam.URI == "" && connectParam.Host == "" && connectParam.Port <= 0 {
			return servererror.NewClientError("the milvus uri is empty")
		}

		if connectParam.URI == "" {
			if connectParam.Host == "" {
				return servererror.NewClientError("the milvus host is empty")
			}
			if connectParam.Port <= 0 {
				return servererror.NewClientError("the milvus port is less or equal zero")
			}
		}

		if (connectParam.Username != "" && connectParam.Password == "") ||
			(connectParam.Username == "" && connectParam.Password != "") {
			return servererror.NewClientError("cannot set only one of the milvus username and password")
		}
	}

	if connectParam.ConnectTimeout < 0 {
		return servererror.NewClientError("the milvus connect timeout is less zero")
	}
	cacheParam := req.BufferConfig
	if cacheParam.Period < 0 {
		return servererror.NewClientError("the cache period is less zero")
	}
	if cacheParam.Size < 0 {
		return servererror.NewClientError("the cache size is less zero")
	}

	if err := e.checkCollectionInfos(req.CollectionInfos); err != nil {
		return err
	}
	if req.RPCChannelInfo.Name != "" && req.RPCChannelInfo.Name != e.config.SourceConfig.ReplicateChan {
		return servererror.NewClientError("the rpc channel is invalid, the channel name should be the same as the source config")
	}

	if connectParam.TargetDBType != "bigquery" {
		connectParam.Token = GetMilvusToken(connectParam)
		connectParam.URI = GetMilvusURI(connectParam)
	}

	var err error
	if req.MilvusConnectParam.TargetDBType == "milvus" {
		_, err = cdcwriter.NewMilvusDataHandler(
			cdcwriter.MilvusURIOption(connectParam.URI),
			cdcwriter.MilvusTokenOption(connectParam.Token),
			cdcwriter.MilvusIgnorePartitionOption(connectParam.IgnorePartition),
			cdcwriter.MilvusConnectTimeoutOption(connectParam.ConnectTimeout),
			cdcwriter.MilvusDialConfigOption(connectParam.DialConfig),
		)
	} else {
		log.Info("start to connect the target db")
		_, err = cdcwriter.NewDataHandler(
			cdcwriter.AgentHost(connectParam.AgentHost),
			cdcwriter.AgentPort(connectParam.AgentPort),
			cdcwriter.URIOption(connectParam.URI),
			cdcwriter.TokenOption(connectParam.Token),
			cdcwriter.ConnectTimeoutOption(connectParam.ConnectTimeout),
			cdcwriter.DialConfigOption(connectParam.DialConfig),
		)
	}
	if err != nil {
		log.Warn(fmt.Sprintf("fail to connect the %s", req.MilvusConnectParam.TargetDBType), zap.Any("connect_param", connectParam), zap.Error(err))
		return errors.WithMessage(err, fmt.Sprintf("fail to connect the %s", req.MilvusConnectParam.TargetDBType))
	}
	return nil
}

func (e *MetaCDC) checkCollectionInfos(infos []model.CollectionInfo) error {
	if len(infos) == 0 {
		return servererror.NewClientError("empty collection info")
	}

	// if len(infos) != 1 || infos[0].Name != cdcreader.AllCollection {
	// 	return servererror.NewClientError("the collection info should be only one, and the collection name should be `*`. Specifying collection name will be supported in the future.")
	// }
	// return nil

	if len(infos) != 1 {
		return servererror.NewClientError("the collection info should be only one.")
	}

	var (
		longNames []string
		emptyName bool
	)
	for _, info := range infos {
		if info.Name == "" {
			emptyName = true
		}
		if info.Name == cdcreader.AllCollection && len(infos) > 1 {
			return servererror.NewClientError(fmt.Sprintf("make sure the only one collection if you want to use the '*' collection param, current param: %v",
				lo.Map(infos, func(t model.CollectionInfo, _ int) string {
					return t.Name
				})))
		}
		if info.Name == cdcreader.AllCollection && len(info.Positions) > 0 {
			// because the position info can't include the collection name when the collection name is `*`
			return servererror.NewClientError("the collection name is `*`, the positions should be empty")
		}
		if len(info.Name) > e.config.MaxNameLength {
			longNames = append(longNames, info.Name)
		}
		for positionChannel := range info.Positions {
			if !cdcreader.IsVirtualChannel(positionChannel) {
				return servererror.NewClientError(fmt.Sprintf("the position channel name is not virtual channel, %s", positionChannel))
			}
		}
	}
	if !emptyName && len(longNames) == 0 {
		return nil
	}
	var errMsg string
	if emptyName {
		errMsg += "there is a collection name that is empty. "
	}
	if len(longNames) > 0 {
		errMsg += fmt.Sprintf("there are some collection names whose length exceeds %d characters, %v", e.config.MaxNameLength, longNames)
	}
	return servererror.NewClientError(errMsg)
}

func (e *MetaCDC) getUUID() string {
	uid := uuid.Must(uuid.NewRandom())
	return strings.ReplaceAll(uid.String(), "-", "")
}

func (e *MetaCDC) startInternal(info *meta.TaskInfo, ignoreUpdateState bool) error {
	taskLog := log.With(zap.String("task_id", info.TaskID))
	milvusURI := GetMilvusURI(info.MilvusConnectParam)
	e.replicateEntityMap.RLock()
	replicateEntity, ok := e.replicateEntityMap.data[milvusURI]
	e.replicateEntityMap.RUnlock()

	if !ok {
		var err error
		replicateEntity, err = e.newReplicateEntity(info)
		if err != nil {
			return err
		}
	}

	ctx := context.Background()
	taskPositions, err := e.metaStoreFactory.GetTaskCollectionPositionMetaStore(ctx).Get(ctx, &meta.TaskCollectionPosition{TaskID: info.TaskID}, nil)
	if err != nil {
		taskLog.Warn("fail to get the task collection position", zap.Error(err))
		return servererror.NewServerError(errors.WithMessage(err, "fail to get the task collection position"))
	}

	channelSeekPosition := make(map[int64]map[string]*msgpb.MsgPosition)
	for _, taskPosition := range taskPositions {
		collectionSeekPosition := make(map[string]*msgpb.MsgPosition)
		// the positionChannel is pchannel name
		for positionChannel, positionInfo := range taskPosition.Positions {
			positionTs := uint64(0)
			if positionInfo.Time > 0 {
				positionTs = tsoutil.ComposeTS(positionInfo.Time+1, 0)
			}
			collectionSeekPosition[positionChannel] = &msgpb.MsgPosition{
				ChannelName: positionChannel,
				MsgID:       positionInfo.DataPair.Data,
				Timestamp:   positionTs,
			}
		}
		channelSeekPosition[taskPosition.CollectionID] = collectionSeekPosition
	}

	collectionReader, err := cdcreader.NewCollectionReader(info.TaskID,
		replicateEntity.channelManager, replicateEntity.metaOp,
		channelSeekPosition, GetShouldReadFunc(info),
		config.ReaderConfig{
			Retry: e.config.Retry,
		})
	if err != nil {
		taskLog.Warn("fail to new the collection reader", zap.Error(err))
		return servererror.NewServerError(errors.WithMessage(err, "fail to new the collection reader"))
	}
	go func() {
		err := <-collectionReader.ErrorChan()
		if err == nil {
			return
		}
		log.Warn("fail to read the message", zap.Error(err))
		_ = e.pauseTaskWithReason(info.TaskID, "fail to read the message, err:"+err.Error(), []meta.TaskState{})
	}()
	rpcRequestChannelName := e.getRPCChannelName(info.RPCRequestChannelInfo)
	rpcRequestPosition := info.RPCRequestChannelInfo.Position
	if rpcRequestPosition == "" && channelSeekPosition[model.ReplicateCollectionID] != nil {
		replicateSeekPosition := channelSeekPosition[model.ReplicateCollectionID][rpcRequestChannelName]
		if replicateSeekPosition != nil {
			rpcRequestPosition = base64.StdEncoding.EncodeToString(replicateSeekPosition.MsgID)
		}
	}
	channelReader, err := e.getChannelReader(info, replicateEntity, rpcRequestChannelName, rpcRequestPosition)
	if err != nil {
		return err
	}
	readCtx, cancelReadFunc := context.WithCancel(log.WithTraceID(context.Background(), info.TaskID))
	replicateEntity.taskQuitFuncs.Insert(info.TaskID, func() {
		collectionReader.QuitRead(readCtx)
		channelReader.QuitRead(readCtx)
		cancelReadFunc()
	})
	replicateEntity.refCnt.Inc()

	if !ignoreUpdateState {
		err = store.UpdateTaskState(e.metaStoreFactory.GetTaskInfoMetaStore(ctx), info.TaskID, meta.TaskStateRunning, []meta.TaskState{meta.TaskStateInitial, meta.TaskStatePaused}, "")
		if err != nil {
			taskLog.Warn("fail to update the task meta", zap.Error(err))
			return servererror.NewServerError(errors.WithMessage(err, "fail to update the task meta, task_id: "+info.TaskID))
		}
	}
	e.cdcTasks.Lock()
	info.State = meta.TaskStateRunning
	info.Reason = ""
	e.cdcTasks.Unlock()

	collectionReader.StartRead(readCtx)
	channelReader.StartRead(readCtx)
	return nil
}

func (e *MetaCDC) newReplicateEntity(info *meta.TaskInfo) (*ReplicateEntity, error) {
	taskLog := log.With(zap.String("task_id", info.TaskID))
	milvusConnectParam := info.MilvusConnectParam

	ctx := context.TODO()
	timeoutCtx, cancelFunc := context.WithTimeout(ctx, time.Duration(milvusConnectParam.ConnectTimeout)*time.Second)
	milvusConnectParam.Token = GetMilvusToken(milvusConnectParam)
	milvusConnectParam.URI = GetMilvusURI(milvusConnectParam)
	milvusAddress := milvusConnectParam.URI

	var milvusClient api.TargetAPI
	var err error

	if strings.ToLower(milvusConnectParam.TargetDBType) == "milvus" {
		milvusClient, err = cdcreader.NewTarget(timeoutCtx, cdcreader.TargetConfig{
			URI:          milvusAddress,
			Token:        milvusConnectParam.Token,
			DialConfig:   milvusConnectParam.DialConfig,
			ProjectId:    milvusConnectParam.ProjectId,
			TargetDBType: milvusConnectParam.TargetDBType,
		})
	} else {
		milvusClient, err = cdcreader.NewTarget(timeoutCtx, cdcreader.TargetConfig{
			URI:          milvusAddress,
			Token:        milvusConnectParam.Token,
			ProjectId:    milvusConnectParam.ProjectId,
			TargetDBType: milvusConnectParam.TargetDBType,
		})
	}

	cancelFunc()
	if err != nil {
		taskLog.Warn("fail to new target", zap.String("address", milvusAddress), zap.Error(err))
		return nil, servererror.NewClientError("fail to connect target milvus server")
	}
	sourceConfig := e.config.SourceConfig
	etcdServerConfig := GetEtcdServerConfigFromSourceConfig(sourceConfig)
	metaOp, err := cdcreader.NewEtcdOp(etcdServerConfig, sourceConfig.DefaultPartitionName, config.EtcdRetryConfig{
		Retry: e.config.Retry,
	}, milvusClient)
	if err != nil {
		taskLog.Warn("fail to new the meta op", zap.Error(err))
		return nil, servererror.NewClientError("fail to new the meta op")
	}

	mqConfig := config.MQConfig{
		Pulsar: e.config.SourceConfig.Pulsar,
		Kafka:  e.config.SourceConfig.Kafka,
	}
	msgDispatcherClient, err := cdcreader.GetMsgDispatcherClient(e.mqFactoryCreator, mqConfig, false)
	if err != nil {
		taskLog.Warn("fail to get the msg dispatcher client", zap.Error(err))
		return nil, servererror.NewClientError("fail to get the msg dispatcher client")
	}

	msgTTDispatcherClient, _ := cdcreader.GetMsgDispatcherClient(e.mqFactoryCreator, mqConfig, true)
	streamFactory, _ := cdcreader.GetStreamFactory(e.mqFactoryCreator, mqConfig, false)

	// default value: 10
	bufferSize := e.config.SourceConfig.ReadChanLen
	ttInterval := e.config.SourceConfig.TimeTickInterval
	channelManager, err := cdcreader.NewReplicateChannelManagerWithDispatchClient(
		msgTTDispatcherClient,
		streamFactory,
		milvusClient,
		config.ReaderConfig{
			MessageBufferSize: bufferSize,
			TTInterval:        ttInterval,
			Retry:             e.config.Retry,
		}, metaOp, func(s string, pack *msgstream.MsgPack) {
			replicateMetric(info, s, pack, metrics.OPTypeRead)
		})
	if err != nil {
		taskLog.Warn("fail to create replicate channel manager", zap.Error(err))
		return nil, servererror.NewClientError("fail to create replicate channel manager")
	}
	targetConfig := milvusConnectParam
	var writerObj api.Writer

	if strings.ToLower(targetConfig.TargetDBType) == "milvus" {
		dataHandler, err := cdcwriter.NewMilvusDataHandler(
			cdcwriter.MilvusURIOption(targetConfig.URI),
			cdcwriter.MilvusTokenOption(targetConfig.Token),
			cdcwriter.MilvusIgnorePartitionOption(targetConfig.IgnorePartition),
			cdcwriter.MilvusConnectTimeoutOption(targetConfig.ConnectTimeout),
			cdcwriter.MilvusDialConfigOption(targetConfig.DialConfig),
		)
		if err != nil {
			taskLog.Warn("fail to new the data handler", zap.Error(err))
			return nil, servererror.NewClientError("fail to new the data handler, task_id: ")
		}
		writerObj = cdcwriter.NewChannelWriter(dataHandler, config.WriterConfig{
			MessageBufferSize: bufferSize,
			Retry:             e.config.Retry,
		}, metaOp.GetAllDroppedObj())
	} else {
		dataHandler, err := cdcwriter.NewDataHandler(
			cdcwriter.AgentHost(targetConfig.AgentHost),
			cdcwriter.AgentPort(targetConfig.AgentPort),
			cdcwriter.TargetDBTypeOption(targetConfig.TargetDBType),
			cdcwriter.URIOption(targetConfig.URI),
			cdcwriter.TokenOption(targetConfig.Token),
			cdcwriter.ConnectTimeoutOption(targetConfig.ConnectTimeout),
			cdcwriter.DialConfigOption(targetConfig.DialConfig),
		)
		if err != nil {
			taskLog.Warn("fail to new the data handler", zap.Error(err))
			return nil, servererror.NewClientError("fail to new the data handler, task_id: ")
		}
		writerObj = cdcwriter.NewChannelWriter(dataHandler, config.WriterConfig{
			MessageBufferSize: bufferSize,
			Retry:             e.config.Retry,
		}, metaOp.GetAllDroppedObj())
	}

	e.replicateEntityMap.Lock()
	defer e.replicateEntityMap.Unlock()
	entity, ok := e.replicateEntityMap.data[milvusAddress]
	if !ok {
		replicateCtx, cancelReplicateFunc := context.WithCancel(ctx)
		channelManager.SetCtx(replicateCtx)
		entity = &ReplicateEntity{
			targetClient:   milvusClient,
			channelManager: channelManager,
			metaOp:         metaOp,
			writerObj:      writerObj,
			entityQuitFunc: cancelReplicateFunc,
			mqDispatcher:   msgDispatcherClient,
			mqTTDispatcher: msgTTDispatcherClient,
			taskQuitFuncs:  typeutil.NewConcurrentMap[string, func()](),
		}
		e.replicateEntityMap.data[milvusAddress] = entity
		e.startReplicateAPIEvent(replicateCtx, info, entity)
		e.startReplicateDMLChannel(replicateCtx, info, entity)
	}
	return entity, nil
}

func (e *MetaCDC) startReplicateAPIEvent(replicateCtx context.Context, info *meta.TaskInfo, entity *ReplicateEntity) {
	go func() {
		taskLog := log.With(zap.String("task_id", info.TaskID))
		for {
			select {
			case <-replicateCtx.Done():
				log.Warn("event chan, the replicate context has closed")
				return
			case replicateAPIEvent, ok := <-entity.channelManager.GetEventChan():
				if !ok {
					taskLog.Warn("the replicate api event channel has closed")
					return
				}
				if replicateAPIEvent.EventType == api.ReplicateError {
					taskLog.Warn("receive the error event", zap.Any("event", replicateAPIEvent))
					_ = e.pauseTaskWithReason(info.TaskID, "fail to read the replicate event", []meta.TaskState{})
					return
				}
				if !e.isRunningTask(info.TaskID) {
					taskLog.Warn("not running task", zap.Any("event", replicateAPIEvent))
					return
				}
				err := entity.writerObj.HandleReplicateAPIEvent(replicateCtx, replicateAPIEvent)
				if err != nil {
					taskLog.Warn("fail to handle replicate event", zap.Any("event", replicateAPIEvent), zap.Error(err))
					_ = e.pauseTaskWithReason(info.TaskID, "fail to handle the replicate event, err: "+err.Error(), []meta.TaskState{})
					return
				}
				metrics.APIExecuteCountVec.WithLabelValues(info.TaskID, replicateAPIEvent.EventType.String()).Inc()
			}
		}
	}()
}

func (e *MetaCDC) startReplicateDMLChannel(replicateCtx context.Context, info *meta.TaskInfo, entity *ReplicateEntity) {
	go func() {
		taskLog := log.With(zap.String("task_id", info.TaskID))
		for {
			select {
			case <-replicateCtx.Done():
				log.Warn("channel chan, the replicate context has closed")
				return
			case channelName, ok := <-entity.channelManager.GetChannelChan():
				taskLog.Info("start to replicate channel", zap.String("channel", channelName))
				if !ok {
					taskLog.Warn("the channel name channel has closed")
					return
				}
				if !e.isRunningTask(info.TaskID) {
					taskLog.Warn("not running task")
					return
				}
				e.startReplicateDMLMsg(replicateCtx, info, entity, channelName)
			}
		}
	}()
}

func (e *MetaCDC) startReplicateDMLMsg(replicateCtx context.Context, info *meta.TaskInfo, entity *ReplicateEntity, channelName string) {
	go func() {
		taskLog := log.With(zap.String("task_id", info.TaskID))
		writeCallback := NewWriteCallback(e.metaStoreFactory, e.rootPath, info.TaskID)

		msgChan := entity.channelManager.GetMsgChan(channelName)
		if msgChan == nil {
			log.Warn("not found the message channel", zap.String("channel", channelName))
			return
		}
		for {
			select {
			case <-replicateCtx.Done():
				log.Warn("msg chan, the replicate context has closed")
				return
			case replicateMsg, ok := <-msgChan:
				if !ok {
					taskLog.Warn("the data channel has closed")
					return
				}
				if !e.isRunningTask(info.TaskID) {
					taskLog.Warn("not running task", zap.Any("pack", replicateMsg))
					return
				}
				msgPack := replicateMsg.MsgPack
				if msgPack == nil {
					log.Warn("the message pack is nil, the task may be stopping")
					return
				}
				if replicateMsg.CollectionName == "" || replicateMsg.CollectionID == 0 {
					taskLog.Warn("fail to handle the replicate message",
						zap.String("collection_name", replicateMsg.CollectionName), zap.Int64("collection_id", replicateMsg.CollectionID))
					_ = e.pauseTaskWithReason(info.TaskID, "fail to handle replicate message, invalid collection name or id", []meta.TaskState{})
					return
				}
				pChannel := msgPack.EndPositions[0].GetChannelName()
				if cdcreader.IsVirtualChannel(pChannel) {
					pChannel = funcutil.ToPhysicalChannel(pChannel)
				}
				position, targetPosition, err := entity.writerObj.HandleReplicateMessage(replicateCtx, pChannel, msgPack)
				if err != nil {
					taskLog.Warn("fail to handle the replicate message", zap.Any("pack", msgPack), zap.Error(err))
					_ = e.pauseTaskWithReason(info.TaskID, "fail to handle replicate message, err:"+err.Error(), []meta.TaskState{})
					return
				}
				msgTime, _ := tsoutil.ParseHybridTs(msgPack.EndTs)
				replicateMetric(info, channelName, msgPack, metrics.OPTypeWrite)

				metaPosition := &meta.PositionInfo{
					Time: msgTime,
					DataPair: &commonpb.KeyDataPair{
						Key:  channelName,
						Data: position,
					},
				}
				var metaOpPosition *meta.PositionInfo
				if len(msgPack.Msgs) > 0 && msgPack.Msgs[0].Type() != commonpb.MsgType_TimeTick {
					metaOpPosition = metaPosition
					metrics.APIExecuteCountVec.WithLabelValues(info.TaskID, "ReplicateMessage").Inc()
				}
				metaTargetPosition := &meta.PositionInfo{
					Time: msgTime,
					DataPair: &commonpb.KeyDataPair{
						Key:  pChannel,
						Data: targetPosition,
					},
				}
				if position != nil {
					msgCollectionName := replicateMsg.CollectionName
					msgCollectionID := replicateMsg.CollectionID
					err = writeCallback.UpdateTaskCollectionPosition(msgCollectionID, msgCollectionName, channelName,
						metaPosition, metaOpPosition, metaTargetPosition)
					if err != nil {
						log.Warn("fail to update the collection position", zap.Any("pack", msgPack), zap.Error(err))
						_ = e.pauseTaskWithReason(info.TaskID, "fail to update task position, err:"+err.Error(), []meta.TaskState{})
						return
					}
				}
			}
		}
	}()
}

func replicateMetric(info *meta.TaskInfo, channelName string, msgPack *msgstream.MsgPack, op string) {
	msgTime, _ := tsoutil.ParseHybridTs(msgPack.EndTs)
	metrics.ReplicateTimeVec.
		WithLabelValues(info.TaskID, channelName, op).
		Set(float64(msgTime))
	var packSize int
	for _, msg := range msgPack.Msgs {
		packSize += msg.Size()
		switch realMsg := msg.(type) {
		case *msgstream.InsertMsg:
			metrics.ReplicateDataCntVec.WithLabelValues(info.TaskID,
				strconv.FormatInt(realMsg.GetCollectionID(), 10), realMsg.GetCollectionName(), op, "insert").Add(float64(realMsg.GetNumRows()))
		case *msgstream.DeleteMsg:
			metrics.ReplicateDataCntVec.WithLabelValues(info.TaskID,
				strconv.FormatInt(realMsg.GetCollectionID(), 10), realMsg.GetCollectionName(), op, "delete").Add(float64(realMsg.GetNumRows()))
		}
	}
	metrics.ReplicateDataSizeVec.WithLabelValues(info.TaskID, channelName, op).Add(float64(packSize))
}

func (e *MetaCDC) getChannelReader(info *meta.TaskInfo, replicateEntity *ReplicateEntity, channelName, channelPosition string) (api.Reader, error) {
	taskLog := log.With(zap.String("task_id", info.TaskID))
	collectionName := info.CollectionNames()[0]
	isAnyCollection := collectionName == cdcreader.AllCollection
	// isTmpCollection := collectionName == model.TmpCollectionName

	dataHandleFunc := func(funcCtx context.Context, pack *msgstream.MsgPack) bool {
		if !e.isRunningTask(info.TaskID) {
			taskLog.Warn("not running task", zap.Any("pack", pack))
			return false
		}
		msgTime, _ := tsoutil.ParseHybridTs(pack.EndTs)

		metrics.ReplicateTimeVec.
			WithLabelValues(info.TaskID, channelName, metrics.OPTypeRead).
			Set(float64(msgTime))

		msgCollectionName := util.GetCollectionNameFromMsgPack(pack)
		// TODO it should be changed if replicate the user and role info or multi collection
		if !isAnyCollection && msgCollectionName != collectionName {
			// skip the message if the collection name is not equal to the task collection name
			return true
		}

		positionBytes, err := replicateEntity.writerObj.HandleOpMessagePack(funcCtx, pack)
		if err != nil {
			taskLog.Warn("fail to handle the op message pack", zap.Any("pack", pack), zap.Error(err))
			_ = e.pauseTaskWithReason(info.TaskID, "fail to handle the op message pack, err:"+err.Error(), []meta.TaskState{})
			return false
		}

		metrics.ReplicateTimeVec.
			WithLabelValues(info.TaskID, channelName, metrics.OPTypeWrite).
			Set(float64(msgTime))
		metrics.APIExecuteCountVec.WithLabelValues(info.TaskID, pack.Msgs[0].Type().String()).Inc()

		rpcChannelName := e.getRPCChannelName(info.RPCRequestChannelInfo)
		metaPosition := &meta.PositionInfo{
			Time: msgTime,
			DataPair: &commonpb.KeyDataPair{
				Key:  rpcChannelName,
				Data: positionBytes,
			},
		}
		writeCallback := NewWriteCallback(e.metaStoreFactory, e.rootPath, info.TaskID)
		err = writeCallback.UpdateTaskCollectionPosition(model.ReplicateCollectionID, model.ReplicateCollectionName, channelName,
			metaPosition, metaPosition, nil)
		if err != nil {
			log.Warn("fail to update the collection position", zap.Any("pack", pack), zap.Error(err))
			_ = e.pauseTaskWithReason(info.TaskID, "fail to update task position, err:"+err.Error(), []meta.TaskState{})
			return false
		}
		return true
	}

	channelReader, err := cdcreader.NewChannelReader(channelName, channelPosition, replicateEntity.mqDispatcher, info.TaskID, dataHandleFunc)
	if err != nil {
		taskLog.Warn("fail to new the channel reader", zap.Error(err))
		return nil, servererror.NewServerError(errors.WithMessage(err, "fail to new the channel reader"))
	}
	return channelReader, nil
}

func (e *MetaCDC) isRunningTask(taskID string) bool {
	e.cdcTasks.RLock()
	defer e.cdcTasks.RUnlock()
	task, ok := e.cdcTasks.data[taskID]
	if !ok {
		return false
	}
	return task.State == meta.TaskStateRunning
}

func (e *MetaCDC) pauseTaskWithReason(taskID, reason string, currentStates []meta.TaskState) error {
	log.Info("pause task", zap.String("task_id", taskID), zap.String("reason", reason))
	err := store.UpdateTaskState(
		e.metaStoreFactory.GetTaskInfoMetaStore(context.Background()),
		taskID,
		meta.TaskStatePaused,
		currentStates,
		reason)
	if err != nil {
		log.Warn("fail to update task reason", zap.String("task_id", taskID), zap.String("reason", reason))
	}
	e.cdcTasks.Lock()
	cdcTask := e.cdcTasks.data[taskID]
	if cdcTask == nil {
		e.cdcTasks.Unlock()
		return err
	}
	cdcTask.State = meta.TaskStatePaused
	cdcTask.Reason = reason
	e.cdcTasks.Unlock()

	milvusURI := GetMilvusURI(cdcTask.MilvusConnectParam)
	e.replicateEntityMap.Lock()
	if replicateEntity, ok := e.replicateEntityMap.data[milvusURI]; ok {
		if quitFunc, ok := replicateEntity.taskQuitFuncs.GetAndRemove(taskID); ok {
			quitFunc()
		}
	}
	delete(e.replicateEntityMap.data, milvusURI)
	e.replicateEntityMap.Unlock()
	return err
}

func (e *MetaCDC) Delete(req *request.DeleteRequest) (*request.DeleteResponse, error) {
	e.cdcTasks.RLock()
	_, ok := e.cdcTasks.data[req.TaskID]
	e.cdcTasks.RUnlock()
	if !ok {
		return nil, servererror.NewClientError("not found the task, task_id: " + req.TaskID)
	}

	err := e.delete(req.TaskID)
	if err != nil {
		return nil, servererror.NewServerError(err)
	}
	return &request.DeleteResponse{}, nil
}

func (e *MetaCDC) delete(taskID string) error {
	e.cdcTasks.RLock()
	_, ok := e.cdcTasks.data[taskID]
	e.cdcTasks.RUnlock()
	if !ok {
		return errors.Errorf("not found the task, task_id: " + taskID)
	}

	var err error
	var info *meta.TaskInfo

	info, err = store.DeleteTask(e.metaStoreFactory, taskID)
	if err != nil {
		return errors.WithMessage(err, "fail to delete the task meta, task_id: "+taskID)
	}
	milvusURI := GetMilvusURI(info.MilvusConnectParam)
	collectionNames := info.CollectionNames()
	e.collectionNames.Lock()
	if collectionNames[0] == cdcreader.AllCollection {
		e.collectionNames.excludeData[milvusURI] = []string{}
	}
	e.collectionNames.data[milvusURI] = lo.Without(e.collectionNames.data[milvusURI], collectionNames...)
	e.collectionNames.Unlock()

	e.cdcTasks.Lock()
	delete(e.cdcTasks.data, taskID)
	e.cdcTasks.Unlock()

	e.replicateEntityMap.Lock()
	if replicateEntity, ok := e.replicateEntityMap.data[milvusURI]; ok {
		if quitFunc, ok := replicateEntity.taskQuitFuncs.GetAndRemove(taskID); ok {
			quitFunc()
			replicateEntity.refCnt.Dec()
		}
		if replicateEntity.refCnt.Load() == 0 {
			replicateEntity.entityQuitFunc()
		}
	}
	delete(e.replicateEntityMap.data, milvusURI)
	e.replicateEntityMap.Unlock()

	return err
}

func (e *MetaCDC) Pause(req *request.PauseRequest) (*request.PauseResponse, error) {
	e.cdcTasks.RLock()
	cdcTask, ok := e.cdcTasks.data[req.TaskID]
	if !ok {
		e.cdcTasks.RUnlock()
		return nil, servererror.NewClientError("not found the task, task_id: " + req.TaskID)
	}
	if cdcTask.State == meta.TaskStatePaused {
		e.cdcTasks.RUnlock()
		return nil, servererror.NewClientError("the task has paused, task_id: " + req.TaskID)
	}
	e.cdcTasks.RUnlock()

	err := e.pauseTaskWithReason(req.TaskID, "manually pause through http interface", []meta.TaskState{meta.TaskStateRunning})
	if err != nil {
		return nil, servererror.NewServerError(errors.WithMessage(err, "fail to update the task state, task_id: "+req.TaskID))
	}

	return &request.PauseResponse{}, err
}

func (e *MetaCDC) Resume(req *request.ResumeRequest) (*request.ResumeResponse, error) {
	e.cdcTasks.RLock()
	cdcTask, ok := e.cdcTasks.data[req.TaskID]
	if !ok {
		e.cdcTasks.RUnlock()
		return nil, servererror.NewClientError("not found the task, task_id: " + req.TaskID)
	}
	if cdcTask.State == meta.TaskStateRunning {
		e.cdcTasks.RUnlock()
		return nil, servererror.NewClientError("the task has running, task_id: " + req.TaskID)
	}
	e.cdcTasks.RUnlock()

	if err := e.startInternal(cdcTask, false); err != nil {
		log.Warn("fail to start the task", zap.Error(err))
		return nil, servererror.NewServerError(errors.WithMessage(err, "fail to start the task, task_id: "+req.TaskID))
	}

	return &request.ResumeResponse{}, nil
}

func (e *MetaCDC) Get(req *request.GetRequest) (*request.GetResponse, error) {
	if req.TaskID == "" {
		return nil, servererror.NewClientError("task_id is empty")
	}
	taskInfo, err := store.GetTaskInfo(e.metaStoreFactory.GetTaskInfoMetaStore(context.Background()), req.TaskID)
	if err != nil {
		if errors.Is(err, servererror.NotFoundErr) {
			return nil, servererror.NewClientError(err.Error())
		}
		return nil, servererror.NewServerError(err)
	}
	return &request.GetResponse{
		Task: request.GetTask(taskInfo),
	}, nil
}

func (e *MetaCDC) GetPosition(req *request.GetPositionRequest) (*request.GetPositionResponse, error) {
	ctx := context.Background()
	positions, err := e.metaStoreFactory.GetTaskCollectionPositionMetaStore(ctx).Get(ctx, &meta.TaskCollectionPosition{TaskID: req.TaskID}, nil)
	if err != nil {
		return nil, servererror.NewServerError(err)
	}
	resp := &request.GetPositionResponse{}
	for _, position := range positions {
		for s, info := range position.Positions {
			msgID, err := EncodeMetaPosition(info)
			if err != nil {
				return nil, servererror.NewServerError(err)
			}
			resp.Positions = append(resp.Positions, request.Position{
				ChannelName: s,
				Time:        info.Time,
				MsgID:       msgID,
			})
		}
		for s, info := range position.OpPositions {
			msgID, err := EncodeMetaPosition(info)
			if err != nil {
				return nil, servererror.NewServerError(err)
			}
			resp.OpPositions = append(resp.OpPositions, request.Position{
				ChannelName: s,
				Time:        info.Time,
				MsgID:       msgID,
			})
		}
		for s, info := range position.TargetPositions {
			msgID, err := EncodeMetaPosition(info)
			if err != nil {
				return nil, servererror.NewServerError(err)
			}
			resp.TargetPositions = append(resp.TargetPositions, request.Position{
				ChannelName: s,
				Time:        info.Time,
				MsgID:       msgID,
			})
		}
	}
	return resp, nil
}

func EncodeMetaPosition(position *meta.PositionInfo) (string, error) {
	msgPosition := &msgpb.MsgPosition{
		ChannelName: position.DataPair.Key,
		MsgID:       position.DataPair.Data,
	}
	positionBytes, err := proto.Marshal(msgPosition)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(positionBytes), nil
}

func (e *MetaCDC) List(req *request.ListRequest) (*request.ListResponse, error) {
	taskInfos, err := store.GetAllTaskInfo(e.metaStoreFactory.GetTaskInfoMetaStore(context.Background()))
	if err != nil && !errors.Is(err, servererror.NotFoundErr) {
		return nil, servererror.NewServerError(err)
	}
	return &request.ListResponse{
		Tasks: lo.Map(taskInfos, func(t *meta.TaskInfo, _ int) request.Task {
			return request.GetTask(t)
		}),
	}, nil
}

func (e *MetaCDC) Maintenance(req *request.MaintenanceRequest) (*request.MaintenanceResponse, error) {
	return maintenance.Handle(req)
}

func GetShouldReadFunc(taskInfo *meta.TaskInfo) cdcreader.ShouldReadFunc {
	isAll := taskInfo.CollectionInfos[0].Name == cdcreader.AllCollection
	return func(collectionInfo *pb.CollectionInfo) bool {
		currentCollectionName := collectionInfo.Schema.Name
		notStarContains := !isAll && lo.ContainsBy(taskInfo.CollectionInfos, func(taskCollectionInfo model.CollectionInfo) bool {
			return taskCollectionInfo.Name == currentCollectionName
		})
		starContains := isAll && !lo.ContainsBy(taskInfo.ExcludeCollections, func(s string) bool {
			return s == currentCollectionName
		})

		return notStarContains || starContains
	}
}
