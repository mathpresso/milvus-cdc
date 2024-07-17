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
	"cloud.google.com/go/bigquery"
	"context"
	"database/sql"
	"database/sql/driver"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"google.golang.org/api/iterator"
	"strings"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-sdk-go/v2/client"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/util"
)

var _ api.TargetAPI = (*TargetClient)(nil)

type TargetClient struct {
	milvusClient   client.Client
	config         TargetConfig
	mysqlClient    driver.Connector
	bigqueryClient bigquery.Client
}

type TargetConfig struct {
	TargetDBType string
	Address      string
	Username     string
	Password     string
	ProjectId    string
	APIKey       string
	EnableTLS    bool
	DialConfig   util.DialConfig
}

func NewTarget(ctx context.Context, config TargetConfig) (api.TargetAPI, error) {
	targetClient := &TargetClient{
		config: config,
	}

	log.Info("new target", zap.Any("targetClient", targetClient))
	if strings.ToLower(config.TargetDBType) == "milvus" {
		_, err := targetClient.GetMilvus(ctx, "")
		if err != nil {
			log.Warn("fail to new target client", zap.String("address", config.Address), zap.Error(err))
			return nil, err
		}
	} else if strings.ToLower(config.TargetDBType) == "mysql" {
		_, err := targetClient.GetMySQL(ctx, "")
		if err != nil {
			log.Warn("fail to new target client", zap.String("address", config.Address), zap.Error(err))
			return nil, err
		}
	} else if strings.ToLower(config.TargetDBType) == "bigquery" {
		_, err := targetClient.GetBigQuery(ctx, "")
		if err != nil {
			log.Warn("fail to new target client", zap.String("address", config.ProjectId), zap.Error(err))
			return nil, err
		}
	}

	log.Info("new target client", zap.String("address", config.Address), zap.String("targetDBType", config.TargetDBType), zap.Any("config", targetClient))
	return targetClient, nil
}

func (t *TargetClient) GetMilvus(ctx context.Context, databaseName string) (client.Client, error) {
	apiKey := t.config.APIKey
	if apiKey == "" {
		apiKey = util.GetAPIKey(t.config.Username, t.config.Password)
	}
	milvusClient, err := util.GetMilvusClientManager().GetMilvusClient(ctx, t.config.Address, apiKey, databaseName, t.config.EnableTLS, t.config.DialConfig)
	if err != nil {
		return nil, err
	}
	return milvusClient, nil
}

func (t *TargetClient) GetMySQL(ctx context.Context, databaseName string) (*sql.DB, error) {
	apiKey := t.config.APIKey
	if apiKey == "" {
		apiKey = util.GetAPIKey(t.config.Username, t.config.Password)
	}
	mysqlClient, err := util.GetMilvusClientManager().GetMySQLClient(ctx, t.config.Address, apiKey, databaseName, t.config.EnableTLS, t.config.DialConfig)
	if err != nil {
		return nil, err
	}
	return mysqlClient, nil
}

func (t *TargetClient) GetBigQuery(ctx context.Context, databaseName string) (*bigquery.Client, error) {
	bigqueryClient, err := util.GetMilvusClientManager().GetBigQueryClient(ctx, t.config.ProjectId, databaseName)
	if err != nil {
		return nil, err
	}
	return bigqueryClient, nil
}

func (t *TargetClient) GetCollectionInfo(ctx context.Context, targetDBType, collectionName, databaseName string) (*model.CollectionInfo, error) {
	databaseName, err := t.GetDatabaseName(ctx, collectionName, databaseName)
	if err != nil {
		log.Warn("fail to get database name", zap.Error(err))
		return nil, err
	}

	collectionInfo := &model.CollectionInfo{}
	var collection *entity.Collection

	if strings.ToLower(targetDBType) == "milvus" {
		milvus, err := t.GetMilvus(ctx, databaseName)
		if err != nil {
			log.Warn("fail to get milvus client", zap.String("database", databaseName), zap.Error(err))
			return nil, err
		}

		collection, err = milvus.DescribeCollection(ctx, collectionName)
		if err != nil {
			log.Warn("fail to describe collection", zap.Error(err))
			return nil, err
		}

		tmpCollectionInfo, err := t.GetPartitionInfo(ctx, collectionName, databaseName)
		if err != nil {
			log.Warn("fail to get partition info", zap.Error(err))
			return nil, err
		}

		collectionInfo.Partitions = tmpCollectionInfo.Partitions
	} else {
		collection = &entity.Collection{}
		var channelName string
		if strings.ToLower(targetDBType) == "mysql" {
			channelName = "mysql"
		} else if strings.ToLower(targetDBType) == "bigquery" {
			channelName = "bigquery"
		}
		collection.PhysicalChannels = append(collection.PhysicalChannels, channelName)
		collection.VirtualChannels = append(collection.VirtualChannels, channelName)
	}

	collectionInfo.DatabaseName = databaseName
	collectionInfo.CollectionID = collection.ID
	collectionInfo.CollectionName = collectionName
	collectionInfo.PChannels = collection.PhysicalChannels
	collectionInfo.VChannels = collection.VirtualChannels

	log.Info("GetCollectionInfo", zap.Any("collectionInfo", collectionInfo))

	return collectionInfo, nil
}

func (t *TargetClient) GetPartitionInfo(ctx context.Context, collectionName, databaseName string) (*model.CollectionInfo, error) {
	databaseName, err := t.GetDatabaseName(ctx, collectionName, databaseName)
	if err != nil {
		log.Warn("fail to get database name", zap.Error(err))
		return nil, err
	}
	milvus, err := t.GetMilvus(ctx, databaseName)
	if err != nil {
		log.Warn("fail to get milvus client", zap.String("database", databaseName), zap.Error(err))
		return nil, err
	}

	collectionInfo := &model.CollectionInfo{}
	partition, err := milvus.ShowPartitions(ctx, collectionName)
	if err != nil || len(partition) == 0 {
		log.Warn("failed to show partitions", zap.Error(err))
		return nil, errors.New("fail to show the partitions")
	}
	partitionInfo := make(map[string]int64, len(partition))
	for _, e := range partition {
		partitionInfo[e.Name] = e.ID
	}
	collectionInfo.Partitions = partitionInfo
	return collectionInfo, nil

	return nil, nil
}

func (t *TargetClient) GetDatabaseName(ctx context.Context, collectionName, databaseName string) (string, error) {
	log.Info("GetDatabaseName", zap.String("collectionName", collectionName), zap.String("databaseName", databaseName))
	if !IsDroppedObject(databaseName) {
		log.Warn("database name is not dropped", zap.String("databaseName", databaseName))
		return databaseName, nil
	}
	if strings.ToLower(t.config.TargetDBType) == "milvus" {
		dbLog := log.With(zap.String("collection", collectionName), zap.String("database", databaseName))
		milvus, err := t.GetMilvus(ctx, "")
		if err != nil {
			dbLog.Warn("fail to get milvus client", zap.String("database", databaseName), zap.Error(err))
			return "", err
		}
		databaseNames, err := milvus.ListDatabases(ctx)
		if err != nil {
			dbLog.Warn("fail to milvus list databases", zap.String("database", databaseName), zap.Error(err))
			return "", err
		}
		for _, dbName := range databaseNames {
			dbMilvus, err := t.GetMilvus(ctx, dbName.Name)
			if err != nil {
				dbLog.Warn("fail to get milvus client", zap.String("connect_db", dbName.Name), zap.Error(err))
				return "", err
			}
			collections, err := dbMilvus.ListCollections(ctx)
			if err != nil {
				dbLog.Warn("fail to milvus list collections", zap.String("connect_db", dbName.Name), zap.Error(err))
				return "", err
			}
			for _, collection := range collections {
				if collection.Name == collectionName {
					return dbName.Name, nil
				}
			}
		}
		dbLog.Warn("not found the database", zap.Any("databases", databaseNames))
	} else if strings.ToLower(t.config.TargetDBType) == "mysql" {
		dbLog := log.With(zap.String("table", collectionName), zap.String("database", databaseName))

		var drows, trows *sql.Rows
		dbMySQL, err := t.GetMySQL(ctx, "")
		if err != nil {
			dbLog.Warn("fail to get mysql client", zap.String("database", databaseName), zap.Error(err))
			return "", err
		}
		drows, err = dbMySQL.Query("show databases")
		if err != nil {
			dbLog.Warn("fail to mysql list databases", zap.String("database", databaseName), zap.Error(err))
			return "", err
		}

		defer drows.Close()

		var dbName, tableName string
		for drows.Next() {
			err = drows.Scan(&dbName)
			if err != nil {
				dbLog.Warn("fail to mysql fetch rows", zap.String("connect_db", dbName), zap.Error(err))
				return "", err
			}

			dbLog.Info("databaseName", zap.String("databaseName", dbName))
			_, err = dbMySQL.Exec("use " + dbName)
			if err != nil {
				dbLog.Warn("fail to command mysql use db", zap.String("connect_db", dbName), zap.Error(err))
				return "", err
			}

			trows, err = dbMySQL.Query("show tables")
			if err != nil {
				dbLog.Warn("fail to mysql list tables", zap.String("connect_db", dbName), zap.Error(err))
				return "", err
			}

			for trows.Next() {
				err = trows.Scan(&tableName)
				dbLog.Info("tableName", zap.String("tableName", tableName))
				if tableName == collectionName {
					trows.Close()
					return dbName, nil
				}
			}

			trows.Close()
		}
		dbLog.Warn("not found the database", zap.Any("databases", databaseName))
	} else if strings.ToLower(t.config.TargetDBType) == "bigquery" {
		dbLog := log.With(zap.String("table", collectionName), zap.String("database", databaseName))

		dbBigQuery, err := t.GetBigQuery(ctx, "")
		if err != nil {
			dbLog.Warn("fail to mysql list tables", zap.String("connect_db", databaseName), zap.Error(err))
			return "", err
		}

		// 데이터셋 목록 조회
		datasets := dbBigQuery.Datasets(ctx)
		for {
			dataset, err := datasets.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				dbLog.Warn("failed to list datasets", zap.String("connect_db", dataset.DatasetID), zap.Error(err))
				return "", err
			}

			// 각 데이터셋의 테이블 목록 조회
			tables := dataset.Tables(ctx)
			for {
				tableName, err := tables.Next()
				if tableName.TableID == collectionName {
					return dataset.DatasetID, nil
				}
				if err == iterator.Done {
					break
				}

				if err != nil {
					dbLog.Warn("failed to list tables", zap.String("connect_db", dataset.DatasetID), zap.Error(err))
					return "", err
				}
			}
		}
	}

	return "", util.NotFoundDatabase
}
