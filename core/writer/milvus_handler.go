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

package writer

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/pkg/util/retry"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/util"
)

type MilvusDataHandler struct {
	api.DataHandler

	address         string
	username        string
	password        string
	enableTLS       bool
	ignorePartition bool // sometimes the has partition api is a deny api
	connectTimeout  int
	retryOptions    []retry.Option
	dialConfig      util.DialConfig
}

// NewMilvusDataHandler options must include AddressOption
func NewMilvusDataHandler(options ...config.Option[*MilvusDataHandler]) (*MilvusDataHandler, error) {
	handler := &MilvusDataHandler{
		connectTimeout: 5,
	}
	for _, option := range options {
		option.Apply(handler)
	}
	if handler.address == "" {
		return nil, errors.New("empty milvus address")
	}

	var err error
	timeoutContext, cancel := context.WithTimeout(context.Background(), time.Duration(handler.connectTimeout)*time.Second)
	defer cancel()

	err = handler.milvusOp(timeoutContext, "", func(milvus client.Client) error {
		return nil
	})
	if err != nil {
		log.Warn("fail to new the milvus client", zap.Error(err))
		return nil, err
	}
	handler.retryOptions = util.GetRetryOptions(config.GetCommonConfig().Retry)
	return handler, nil
}

func (m *MilvusDataHandler) milvusOp(ctx context.Context, database string, f func(milvus client.Client) error) error {
	retryMilvusFunc := func(c client.Client) error {
		// TODO Retryable and non-retryable errors should be distinguished
		var err error
		retryErr := retry.Do(ctx, func() error {
			err = f(c)
			return err
		}, m.retryOptions...)
		if retryErr != nil && err != nil {
			return err
		}
		if retryErr != nil {
			return retryErr
		}
		return nil
	}

	milvusClient, err := util.GetMilvusClientManager().GetMilvusClient(ctx, m.address, util.GetAPIKey(m.username, m.password), database, m.enableTLS, m.dialConfig)
	if err != nil {
		log.Warn("fail to get milvus client", zap.Error(err))
		return err
	}
	return retryMilvusFunc(milvusClient)
}

func (m *MilvusDataHandler) CreateCollection(ctx context.Context, param *api.CreateCollectionParam) error {
	var options []client.CreateCollectionOption
	for _, property := range param.Properties {
		options = append(options, client.WithCollectionProperty(property.GetKey(), property.GetValue()))
	}
	options = append(options,
		client.WithConsistencyLevel(entity.ConsistencyLevel(param.ConsistencyLevel)),
		client.WithCreateCollectionMsgBase(param.Base),
	)
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		if _, err := milvus.DescribeCollection(ctx, param.Schema.CollectionName); err == nil {
			log.Info("skip to create collection, because it's has existed", zap.String("collection", param.Schema.CollectionName))
			return nil
		}
		return milvus.CreateCollection(ctx, param.Schema, param.ShardsNum, options...)
	})
}

func (m *MilvusDataHandler) DropCollection(ctx context.Context, param *api.DropCollectionParam) error {
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.DropCollection(ctx, param.CollectionName, client.WithDropCollectionMsgBase(param.Base))
	})
}

func (m *MilvusDataHandler) Insert(ctx context.Context, param *api.InsertParam) error {
	partitionName := param.PartitionName
	if m.ignorePartition {
		log.Info("ignore partition name in insert request", zap.String("partition", partitionName))
		partitionName = ""
	}

	log.Info("insert request", zap.String("collection", param.CollectionName), zap.String("partition", partitionName), zap.Any("columns", param.Columns))
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		_, err := milvus.Insert(ctx, param.CollectionName, partitionName, param.Columns...)
		return err
	})
}

func (m *MilvusDataHandler) Delete(ctx context.Context, param *api.DeleteParam) error {
	partitionName := param.PartitionName
	if m.ignorePartition {
		log.Info("ignore partition name in delete request", zap.String("partition", partitionName))
		partitionName = ""
	}

	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.DeleteByPks(ctx, param.CollectionName, partitionName, param.Column)
	})
}

func (m *MilvusDataHandler) CreatePartition(ctx context.Context, param *api.CreatePartitionParam) error {
	if m.ignorePartition {
		log.Warn("ignore create partition", zap.String("collection", param.CollectionName), zap.String("partition", param.PartitionName))
		return nil
	}
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		partitions, err := milvus.ShowPartitions(ctx, param.CollectionName)
		if err != nil {
			log.Warn("fail to show partitions", zap.String("collection", param.CollectionName), zap.Error(err))
			return err
		}
		for _, partition := range partitions {
			if partition.Name == param.PartitionName {
				log.Info("skip to create partition, because it's has existed",
					zap.String("collection", param.CollectionName),
					zap.String("partition", param.PartitionName))
				return nil
			}
		}
		return milvus.CreatePartition(ctx, param.CollectionName, param.PartitionName, client.WithCreatePartitionMsgBase(param.Base))
	})
}

func (m *MilvusDataHandler) DropPartition(ctx context.Context, param *api.DropPartitionParam) error {
	if m.ignorePartition {
		log.Warn("ignore drop partition", zap.String("collection", param.CollectionName), zap.String("partition", param.PartitionName))
		return nil
	}
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.DropPartition(ctx, param.CollectionName, param.PartitionName, client.WithDropPartitionMsgBase(param.Base))
	})
}

func (m *MilvusDataHandler) CreateIndex(ctx context.Context, param *api.CreateIndexParam) error {
	indexEntity := entity.NewGenericIndex(param.GetIndexName(), "", util.ConvertKVPairToMap(param.GetExtraParams()))
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.CreateIndex(ctx, param.GetCollectionName(), param.GetFieldName(), indexEntity, true,
			client.WithIndexName(param.GetIndexName()),
			client.WithIndexMsgBase(param.GetBase()),
		)
	})
}

func (m *MilvusDataHandler) DropIndex(ctx context.Context, param *api.DropIndexParam) error {
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.DropIndex(ctx, param.CollectionName, param.FieldName,
			client.WithIndexName(param.IndexName),
			client.WithIndexMsgBase(param.GetBase()),
		)
	})
}

func (m *MilvusDataHandler) LoadCollection(ctx context.Context, param *api.LoadCollectionParam) error {
	// TODO resource group
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.LoadCollection(ctx, param.CollectionName, true,
			client.WithReplicaNumber(param.ReplicaNumber),
			client.WithLoadCollectionMsgBase(param.GetBase()),
		)
	})
}

func (m *MilvusDataHandler) ReleaseCollection(ctx context.Context, param *api.ReleaseCollectionParam) error {
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.ReleaseCollection(ctx, param.CollectionName,
			client.WithReleaseCollectionMsgBase(param.GetBase()),
		)
	})
}

func (m *MilvusDataHandler) LoadPartitions(ctx context.Context, param *api.LoadPartitionsParam) error {
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.LoadPartitions(ctx, param.CollectionName, param.PartitionNames, true,
			client.WithLoadPartitionsMsgBase(param.GetBase()))
	})
}

func (m *MilvusDataHandler) ReleasePartitions(ctx context.Context, param *api.ReleasePartitionsParam) error {
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.ReleasePartitions(ctx, param.CollectionName, param.PartitionNames,
			client.WithReleasePartitionMsgBase(param.GetBase()))
	})
}

func (m *MilvusDataHandler) Flush(ctx context.Context, param *api.FlushParam) error {
	for _, s := range param.GetCollectionNames() {
		if err := m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
			return milvus.Flush(ctx, s, true, client.WithFlushMsgBase(param.GetBase()))
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m *MilvusDataHandler) CreateDatabase(ctx context.Context, param *api.CreateDatabaseParam) error {
	return m.milvusOp(ctx, "", func(milvus client.Client) error {
		return milvus.CreateDatabase(ctx, param.DbName,
			client.WithCreateDatabaseMsgBase(param.GetBase()),
		)
	})
}

func (m *MilvusDataHandler) DropDatabase(ctx context.Context, param *api.DropDatabaseParam) error {
	return m.milvusOp(ctx, "", func(milvus client.Client) error {
		return milvus.DropDatabase(ctx, param.DbName,
			client.WithDropDatabaseMsgBase(param.GetBase()),
		)
	})
}

func (m *MilvusDataHandler) ReplicateMessage(ctx context.Context, param *api.ReplicateMessageParam) error {
	var (
		resp  *entity.MessageInfo
		err   error
		opErr error
	)

	opErr = m.milvusOp(ctx, "", func(milvus client.Client) error {
		resp, err = milvus.ReplicateMessage(ctx, param.ChannelName,
			param.BeginTs, param.EndTs,
			param.MsgsBytes,
			param.StartPositions, param.EndPositions,
			client.WithReplicateMessageMsgBase(param.Base))
		return err
	})
	if err != nil {
		return err
	}
	if opErr != nil {
		return opErr
	}
	param.TargetMsgPosition = resp.Position
	return nil
}

func convertBlobData(blob *commonpb.Blob, convertType string) (interface{}, error) {
	if blob == nil || len(blob.Value) == 0 {
		return nil, fmt.Errorf("empty or nil blob")
	}

	buf := bytes.NewReader(blob.Value)

	if convertType == "floatvector" {
		// int64 변환
		var intValue int64
		if err := binary.Read(buf, binary.LittleEndian, &intValue); err != nil {
			return nil, fmt.Errorf("failed to read int64: %v", err)
		}

		return intValue, nil
	}

	// bool 변환
	var boolValue bool
	if convertType == "bool" {
		if err := binary.Read(buf, binary.LittleEndian, &boolValue); err != nil {
			return nil, fmt.Errorf("failed to read bool: %v", err)
		}
	}

	// varchar 변환
	if convertType == "varchar" {
		stringBytes := make([]byte, 0)
		var stringValue string

		for {
			b, err := buf.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("failed to read string: %v", err)
			}
			if b == 0 {
				break
			}
			stringBytes = append(stringBytes, b)
		}
		stringValue = string(stringBytes)

		return stringValue, nil
	}

	// [][]float32 변환
	if convertType == "floatvector" {
		var floatArrays [][]float32

		for buf.Len() > 0 {
			var floatArray []float32
			for i := 0; i < 3; i++ { // 예제에서는 각 float32 배열이 3개의 요소를 가진다고 가정
				var floatValue float32
				if err := binary.Read(buf, binary.LittleEndian, &floatValue); err != nil {
					return nil, fmt.Errorf("failed to read float32: %v", err)
				}
				floatArray = append(floatArray, floatValue)
			}
			floatArrays = append(floatArrays, floatArray)
		}

		return floatArrays, nil
	}

	return nil, nil
}

/*
func convertUintSliceToIntSlice(uintSlice []uint64) []int64 {
	intSlice := make([]int64, len(uintSlice))
	for i, v := range uintSlice {
		if v > uint64(^int64(0)) {
			return nil
		}
		intSlice[i] = int64(v)
	}
	return intSlice
}
*/
// convertInsertMsgToInsertParam는 msgstream.InsertMsg를 api.InsertParam으로 변환합니다.
func convertInsertMsgToInsertParam(insertMsg *msgstream.InsertMsg) (*api.InsertParam, error) {
	if insertMsg == nil {
		return nil, fmt.Errorf("nil insert message")
	}

	collectionName := insertMsg.CollectionName

	// 각 필드 데이터를 추출하여 entity.Column 타입으로 변환합니다.
	var columns []entity.Column

	// 예제에서는 RowIDs와 Timestamps를 사용합니다.
	//	rowIDColumn := entity.NewColumnInt64("row_id", insertMsg.RowIDs)
	//	timestampColumn := entity.NewColumnInt64("timestamp", convertUintSliceToIntSlice(insertMsg.Timestamps))
	//	columns = append(columns, rowIDColumn, timestampColumn)

	// InsertMsg의 FieldsData를 entity.Column으로 변환합니다.
	for _, fieldData := range insertMsg.FieldsData {
		switch fieldData.Type {
		case schemapb.DataType_Bool:
			values := make([]bool, len(fieldData.GetScalars().GetBoolData().Data))
			for i, v := range fieldData.GetScalars().GetBoolData().Data {
				values[i] = v
			}
			column := entity.NewColumnBool(fieldData.FieldName, values)
			columns = append(columns, column)
		case schemapb.DataType_VarChar:
			values := make([]string, len(fieldData.GetScalars().GetStringData().Data))
			for i, v := range fieldData.GetScalars().GetStringData().Data {
				values[i] = v
			}
			column := entity.NewColumnString(fieldData.FieldName, values)
			columns = append(columns, column)
		case schemapb.DataType_Int64:
			values := make([]int64, len(fieldData.GetScalars().GetLongData().Data))
			for i, v := range fieldData.GetScalars().GetLongData().Data {
				values[i] = v
			}
			column := entity.NewColumnInt64(fieldData.FieldName, values)
			columns = append(columns, column)
		case schemapb.DataType_Float:
			values := make([]float32, len(fieldData.GetScalars().GetFloatData().Data))
			for i, v := range fieldData.GetScalars().GetFloatData().Data {
				values[i] = v
			}
			column := entity.NewColumnFloat(fieldData.FieldName, values)
			columns = append(columns, column)
		case schemapb.DataType_FloatVector:
			dim := int(fieldData.GetVectors().Dim)
			values := make([][]float32, len(fieldData.GetVectors().GetFloatVector().Data)/dim)
			for i := 0; i < len(values); i++ {
				values[i] = fieldData.GetVectors().GetFloatVector().Data[i*dim : (i+1)*dim]
			}
			column := entity.NewColumnFloatVector(fieldData.FieldName, dim, values)
			columns = append(columns, column)
		default:
			return nil, fmt.Errorf("unsupported field type: %v", fieldData.Type)
		}
	}

	insertParam := &api.InsertParam{
		CollectionName: collectionName,
		Columns:        columns,
	}
	return insertParam, nil
}

// convertInsertMsgToInsertParam는 msgstream.InsertMsg를 api.InsertParam으로 변환합니다.
func convertDeleteMsgToDeleteParam(deleteMsg *msgstream.DeleteMsg) (*api.DeleteParam, error) {
	if deleteMsg == nil {
		return nil, fmt.Errorf("nil insert message")
	}

	collectionName := deleteMsg.CollectionName

	// PrimaryKeys 필드를 entity.Column으로 변환합니다.
	var pkColumn entity.Column
	if deleteMsg.PrimaryKeys.GetIntId() != nil {
		pkColumn = entity.NewColumnInt64("row_id", deleteMsg.PrimaryKeys.GetIntId().Data)
	} else if deleteMsg.PrimaryKeys.GetStrId() != nil {
		pkColumn = entity.NewColumnString("row_id", deleteMsg.PrimaryKeys.GetStrId().Data)
	} else {
		return nil, fmt.Errorf("unsupported primary key data type")
	}

	deleteParam := &api.DeleteParam{
		CollectionName: collectionName,
		PartitionName:  deleteMsg.PartitionName,
		Column:         pkColumn,
	}

	return deleteParam, nil
}

func (m *MilvusDataHandler) DescribeCollection(ctx context.Context, param *api.DescribeCollectionParam) error {
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		_, err := milvus.DescribeCollection(ctx, param.Name)
		return err
	})
}

func (m *MilvusDataHandler) DescribeDatabase(ctx context.Context, param *api.DescribeDatabaseParam) error {
	return m.milvusOp(ctx, "", func(milvus client.Client) error {
		databases, err := milvus.ListDatabases(ctx)
		if err != nil {
			return err
		}
		for _, database := range databases {
			if database.Name == param.Name {
				return nil
			}
		}
		return errors.Newf("database [%s] not found", param.Name)
	})
}

func (m *MilvusDataHandler) DescribePartition(ctx context.Context, param *api.DescribePartitionParam) error {
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		partitions, err := milvus.ShowPartitions(ctx, param.CollectionName)
		if err != nil {
			return err
		}
		for _, partition := range partitions {
			if partition.Name == param.PartitionName {
				return nil
			}
		}
		return errors.Newf("partition [%s] not found", param.PartitionName)
	})
}
