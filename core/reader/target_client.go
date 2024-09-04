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

package reader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/pkg/util/resource"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/util"
)

var _ api.TargetAPI = (*TargetClient)(nil)

type TargetClient struct {
	client client.Client
	config TargetConfig
}

type TargetConfig struct {
	TargetDBType      string          `json:"target_db_type"`
	URI               string          `json:"uri"`
	Token             string          `json:"token"`
	ProjectId         string          `json:"project_id"`
	APIKey            string          `json:"api_key"`
	ConnectionTimeout int             `json:"connection_timeout"`
	DialConfig        util.DialConfig `json:"dial_config"`
	TargetCDCAgentUri string          `json:"target_cdc_agent_uri"`
}

func NewTarget(ctx context.Context, config TargetConfig) (api.TargetAPI, error) {
	targetClient := &TargetClient{
		config: config,
	}
	return targetClient, nil
}

func (t *TargetClient) milvusOp(ctx context.Context, database string, f func(milvus client.Client) error) error {
	log.Info("get milvus client", zap.String("uri", t.config.URI), zap.String("Token", t.config.Token), zap.String("database", database))
	c, err := util.GetMilvusClientManager().GetMilvusClient(ctx, t.config.URI, t.config.Token, database, t.config.DialConfig)
	if err != nil {
		log.Warn("fail to get milvus client", zap.Error(err))
		return err
	}
	err = f(c)
	if status.Code(err) == codes.Canceled {
		util.GetMilvusClientManager().DeleteMilvusClient(t.config.URI, database)
		log.Warn("grpc: the client connection is closing, waiting...", zap.Error(err))
		time.Sleep(resource.DefaultExpiration)
	}
	return err
}

func (t *TargetClient) GetCollectionInfo(ctx context.Context, targetDBType, collectionName, databaseName string) (*model.CollectionInfo, error) {
	databaseName, err := t.GetDatabaseName(ctx, collectionName, databaseName)
	if err != nil {
		log.Warn("fail to get database name", zap.Error(err))
		return nil, err
	}

	collectionInfo := &model.CollectionInfo{}
	if strings.ToLower(targetDBType) == "milvus" {
		err = t.milvusOp(ctx, databaseName, func(milvus client.Client) error {
			collection, err := milvus.DescribeCollection(ctx, collectionName)
			if err != nil {
				return err
			}
			collectionInfo.CollectionID = collection.ID
			collectionInfo.PChannels = collection.PhysicalChannels
			collectionInfo.VChannels = collection.VirtualChannels
			return nil
		})
		if err != nil {
			log.Warn("fail to get collection info", zap.Error(err))
			return nil, err
		}

		tmpCollectionInfo, err := t.GetPartitionInfo(ctx, collectionName, databaseName)
		if err != nil {
			log.Warn("fail to get partition info", zap.Error(err))
			return nil, err
		}
		collectionInfo.Partitions = tmpCollectionInfo.Partitions
	}

	collectionInfo.DatabaseName = databaseName
	collectionInfo.CollectionName = collectionName

	return collectionInfo, nil
}

func (t *TargetClient) GetPartitionInfo(ctx context.Context, collectionName, databaseName string) (*model.CollectionInfo, error) {
	var err error
	databaseName, err = t.GetDatabaseName(ctx, collectionName, databaseName)
	if err != nil {
		log.Warn("fail to get database name", zap.Error(err))
		return nil, err
	}
	collectionInfo := &model.CollectionInfo{}
	var partition []*entity.Partition
	err = t.milvusOp(ctx, databaseName, func(milvus client.Client) error {
		partition, err = milvus.ShowPartitions(ctx, collectionName)
		if err != nil {
			return err
		}
		if len(partition) == 0 {
			log.Warn("failed to show partitions", zap.Error(err))
			return errors.New("fail to show the partitions")
		}
		return nil
	})
	if err != nil {
		log.Warn("fail to show partitions", zap.Error(err))
		return nil, err
	}

	partitionInfo := make(map[string]int64, len(partition))
	for _, e := range partition {
		partitionInfo[e.Name] = e.ID
	}
	collectionInfo.Partitions = partitionInfo
	return collectionInfo, nil
}

func (t *TargetClient) GetOtherCollectionInfo(databaseName, collectionName string) (string, error) {
	type TargetConn struct {
		TargetDbType string `json:"target_db_type"`
		Uri          string `json:"uri"`
		ProjectId    string `json:"project_id"`
		User         string `json:"user"`
		Passwd       string `json:"passwd"`
		Token        string `json:"token"`
	}

	type collectionInfo struct {
		TargetConn
		DatabaseName   string `json:"database_name"`
		CollectionName string `json:"collection_name"`
	}

	var tableInfo collectionInfo

	tableInfo.DatabaseName = databaseName
	tableInfo.CollectionName = collectionName
	tableInfo.TargetDbType = t.config.TargetDBType
	tableInfo.Uri = t.config.URI
	tableInfo.ProjectId = t.config.ProjectId

	urlInfo := strings.Split(t.config.TargetCDCAgentUri, ":")[0]
	urlInfo, _ = url.JoinPath(urlInfo, "collection", "info")

	log.Info("target cdc agent", zap.String("uri", urlInfo))
	pbytes, _ := json.Marshal(tableInfo)
	requestBody := bytes.NewReader(pbytes)

	req, err := http.NewRequest("POST", urlInfo, requestBody)
	if err != nil {
		log.Error("failed to make http request information", zap.Error(err))
		return "", err
	}

	req.Header.Set("Content-Type", "application/json")
	defer req.Body.Close()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Error("failed to remote execute", zap.Error(err))
		return "", err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("failed to parse", zap.Error(err))
		return "", err
	}

	type repStatus struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}

	var data repStatus
	json.Unmarshal(body, &data)

	if resp.StatusCode != http.StatusOK {
		if data.Code == http.StatusConflict || data.Code == http.StatusGone {
			return "", errors.New(fmt.Sprintf("%s", data.Message))
		}

		return "", errors.New(fmt.Sprintf("failed to get database for match collection", zap.Int("status code", resp.StatusCode), zap.String("return message", data.Message)))
	}

	return data.Message, nil
}

func (t *TargetClient) GetDatabaseName(ctx context.Context, collectionName, databaseName string) (string, error) {
	if !IsDroppedObject(databaseName) {
		return databaseName, nil
	}
	dbLog := log.With(zap.String("collection", collectionName), zap.String("database", databaseName))

	var databaseNames []entity.Database
	var err error

	if t.config.TargetDBType == "milvus" {
		err = t.milvusOp(ctx, "", func(milvus client.Client) error {
			databaseNames, err = milvus.ListDatabases(ctx)
			return err
		})
		if err != nil {
			dbLog.Warn("fail to list databases", zap.Error(err))
			return "", err
		}

		for _, dbName := range databaseNames {
			var collections []*entity.Collection
			err = t.milvusOp(ctx, dbName.Name, func(dbMilvus client.Client) error {
				collections, err = dbMilvus.ListCollections(ctx)
				return err
			})
			if err != nil {
				dbLog.Warn("fail to list collections", zap.Error(err))
				return "", err
			}

			for _, collection := range collections {
				if collection.Name == collectionName {
					return dbName.Name, nil
				}
			}
		}
	} else {
		if t.config.TargetDBType != "elasticsearch" {
			dbName, err := t.GetOtherCollectionInfo(databaseName, collectionName)
			if err != nil {
				dbLog.Warn("failed to get database", zap.Error(err))
				return "", err
			}

			if dbName != "" {
				return dbName, nil
			}
		}
	}

	dbLog.Warn("not found the database", zap.String("target DB Type", t.config.TargetDBType), zap.Any("databases", databaseNames))
	return "", util.NotFoundDatabase
}
