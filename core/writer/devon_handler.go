package writer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/resource"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/util"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
	"strconv"
	"strings"
	"time"
)

type DataHandler struct {
	api.DataHandler

	targetDBType    string          `json:"target_db_type"`
	agentHost       string          `json:"agent_host"`
	agentPort       string          `json:"agent_port"`
	token           string          `json:"token"`
	uri             string          `json:"uri"`
	address         string          `json:"address"`
	database        string          `json:"database"`
	collection      string          `json:"collection"`
	username        string          `json:"user_name"`
	password        string          `json:"password"`
	enableTLS       bool            `json:"enable_tls"`
	ignorePartition bool            // sometimes the has partition api is a deny api
	connectTimeout  int             `json:"connect_timeout"`
	projectId       string          `json:"project_id"`
	msgBytes        [][]byte        `json:"msg_bytes"`
	retryOptions    []retry.Option  `json:"retry_options"`
	dialConfig      util.DialConfig `json:"dial_config"`
}

func NewDataHandler(options ...config.Option[*DataHandler]) (*DataHandler, error) {
	handler := &DataHandler{
		connectTimeout: 5,
	}
	for _, option := range options {
		option.Apply(handler)
	}
	/*
		if handler.address == "" {
			return nil, errors.New("empty cdc agent address")
		}
	*/
	var err error
	timeoutContext, cancel := context.WithTimeout(context.Background(), time.Duration(handler.connectTimeout)*time.Second)
	defer cancel()

	err = handler.DBOp(timeoutContext, func(db net.Conn) error {
		return nil
	})
	if err != nil {
		log.Warn("fail to new the cdc agent client", zap.Error(err))
		return nil, err
	}
	handler.retryOptions = util.GetRetryOptions(config.GetCommonConfig().Retry)
	return handler, nil
}

func (m *DataHandler) DBOp(ctx context.Context, f func(db net.Conn) error) error {
	retryDBAgentFunc := func(c net.Conn) error {
		var err error
		retryErr := retry.Do(ctx, func() error {
			c, err = util.GetDBClientManager().GetDBClient(ctx, m.agentHost, m.agentPort, m.address, m.database, m.collection, util.DialConfig{}, m.connectTimeout)
			if err != nil {
				log.Warn("fail to get cdc agent client", zap.Error(err))
				return err
			}
			err = f(c)
			if status.Code(err) == codes.Canceled {
				util.GetDBClientManager().DeleteDBClient(m.address, m.database, m.collection)
				log.Warn("grpc: the client connection is closing, waiting...", zap.Error(err))
				time.Sleep(resource.DefaultExpiration)
			}
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

	dbClient, err := util.GetDBClientManager().GetDBClient(ctx, m.agentHost, m.agentPort, m.address, m.database, m.collection, util.DialConfig{}, m.connectTimeout)
	if err != nil {
		log.Warn("fail to get cdc agent client", zap.Error(err))
		return err
	}

	return retryDBAgentFunc(dbClient)
}

func (m *DataHandler) ReplicaMessageHandler(ctx context.Context, param *api.ReplicateMessageParam) error {
	if param.ChannelName != "" {
		m.database = strings.Split(param.ChannelName, "-")[0]
		m.collection = strings.Split(param.ChannelName, "-")[1]
	}

	m.msgBytes = param.MsgsBytes

	messageJSON, err := json.Marshal(m)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	return m.DBOp(ctx, func(dbClient net.Conn) error {
		_, err := dbClient.Write(messageJSON)
		if err != nil {
			log.Warn("failed to send message:", zap.Error(err))
			return err
		}

		return nil
	})
}

func (m *DataHandler) CreateCollection(ctx context.Context, param *api.CreateCollectionParam) error {
	messageJSON, err := json.Marshal(param)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	return m.DBOp(ctx, func(dbClient net.Conn) error {
		_, err := dbClient.Write(messageJSON)
		if err != nil {
			log.Warn("failed to send message:", zap.Error(err))
			return err
		}

		return nil
	})
}

func (m *DataHandler) DropCollection(ctx context.Context, param *api.DropCollectionParam) error {
	messageJSON, err := json.Marshal(param)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	return m.DBOp(ctx, func(dbClient net.Conn) error {
		_, err := dbClient.Write(messageJSON)
		if err != nil {
			log.Warn("failed to send message:", zap.Error(err))
			return err
		}

		return nil
	})
}

func float32SliceToStringSlice(input []float32) []string {
	output := make([]string, len(input))
	for i, v := range input {
		output[i] = strconv.FormatFloat(float64(v), 'f', -1, 32)
	}
	return output
}

func (m *DataHandler) Insert(ctx context.Context, param *api.InsertParam) (err error) {
	columns := []string{}

	rowValues := [][]interface{}{}
	for _, col := range param.Columns {
		columns = append(columns, fmt.Sprintf("`%s`", col.Name()))

		colValues := []interface{}{}
		switch data := col.FieldData().GetField().(type) {
		case *schemapb.FieldData_Scalars:
			switch scalarData := data.Scalars.Data.(type) {
			case *schemapb.ScalarField_LongData:
				for _, v := range scalarData.LongData.Data {
					colValues = append(colValues, v)
				}
			case *schemapb.ScalarField_BoolData:
				for _, v := range scalarData.BoolData.Data {
					colValues = append(colValues, v)
				}
			case *schemapb.ScalarField_StringData:
				for _, v := range scalarData.StringData.Data {
					colValues = append(colValues, fmt.Sprintf("'%s'", v))
				}
			case *schemapb.ScalarField_ArrayData:
				for _, v := range scalarData.ArrayData.Data {
					colValues = append(colValues, fmt.Sprintf("'%s'", v))
				}
			case *schemapb.ScalarField_IntData:
				for _, v := range scalarData.IntData.Data {
					colValues = append(colValues, v)
				}
			case *schemapb.ScalarField_FloatData:
				for _, v := range scalarData.FloatData.Data {
					colValues = append(colValues, v)
				}
			case *schemapb.ScalarField_DoubleData:
				for _, v := range scalarData.DoubleData.Data {
					colValues = append(colValues, v)
				}
			case *schemapb.ScalarField_JsonData:
				for _, v := range scalarData.JsonData.Data {
					colValues = append(colValues, fmt.Sprintf("'%s'", v))
				}
			case *schemapb.ScalarField_BytesData:
				for _, v := range scalarData.BytesData.Data {
					colValues = append(colValues, fmt.Sprintf("'%s'", v))
				}
			default:
				return fmt.Errorf("unsupported scalar data type: %T", scalarData)
			}

			rowValues = append(rowValues, colValues)
		case *schemapb.FieldData_Vectors:
			switch vectorData := data.Vectors.Data.(type) {
			case *schemapb.VectorField_FloatVector:
				dim := data.Vectors.Dim
				cnt := int64(1)
				var vec []float32
				for _, v := range vectorData.FloatVector.Data {
					vec = append(vec, v)

					if cnt == dim {
						colValues = append(colValues, fmt.Sprintf("[%v]", join(float32SliceToStringSlice(vec), ",")))
						vec = []float32{}
						cnt = 1
					} else {
						cnt++
					}
				}

				rowValues = append(rowValues, colValues)
			default:
				return fmt.Errorf("unsupported vector data type: %T", vectorData)
			}
		default:
			return fmt.Errorf("unsupported field data type: %T", data)
		}
	}

	var value string
	var values []string

	for rowCnt := 0; rowCnt < len(rowValues[0]); rowCnt++ {
		for colNo, _ := range columns {
			if colNo == 0 {
				value = fmt.Sprintf("(%s", fmt.Sprintf("%v", rowValues[colNo][rowCnt]))
			} else {
				value = fmt.Sprintf("%s,%s", value, fmt.Sprintf("%v", rowValues[colNo][rowCnt]))
			}
		}
		value = fmt.Sprintf("%s)", value)

		values = append(values, value)
	}

	messageJSON, err := json.Marshal(param)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	err = m.DBOp(ctx, func(dbClient net.Conn) error {
		_, err := dbClient.Write(messageJSON)
		if err != nil {
			log.Warn("failed to send message:", zap.Error(err))
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func StructToReader(s interface{}) (*bytes.Reader, error) {
	// 구조체를 JSON으로 직렬화
	data, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("error marshalling struct: %v", err)
	}

	// *bytes.Reader로 변환하여 반환
	reader := bytes.NewReader(data)
	return reader, nil
}

func (m *DataHandler) Delete(ctx context.Context, param *api.DeleteParam) error {
	messageJSON, err := json.Marshal(param)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	return m.DBOp(ctx, func(dbClient net.Conn) error {
		_, err := dbClient.Write(messageJSON)
		if err != nil {
			log.Warn("failed to send message:", zap.Error(err))
			return err
		}

		return nil
	})
}

func (m *DataHandler) CreateIndex(ctx context.Context, param *api.CreateIndexParam) error {
	messageJSON, err := json.Marshal(param)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	return m.DBOp(ctx, func(dbClient net.Conn) error {
		_, err := dbClient.Write(messageJSON)
		if err != nil {
			log.Warn("failed to send message:", zap.Error(err))
			return err
		}

		return nil
	})
}

func (m *DataHandler) DropIndex(ctx context.Context, param *api.DropIndexParam) error {
	// BigQuery는 인덱스를 지원하지 않습니다.
	return nil
}

func (m *DataHandler) CreateDatabase(ctx context.Context, param *api.CreateDatabaseParam) error {
	messageJSON, err := json.Marshal(param)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	return m.DBOp(ctx, func(dbClient net.Conn) error {
		_, err := dbClient.Write(messageJSON)
		if err != nil {
			log.Warn("failed to send message:", zap.Error(err))
			return err
		}

		return nil
	})
}

func (m *DataHandler) DropDatabase(ctx context.Context, param *api.DropDatabaseParam) error {
	messageJSON, err := json.Marshal(param)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	return m.DBOp(ctx, func(dbClient net.Conn) error {
		_, err := dbClient.Write(messageJSON)
		if err != nil {
			log.Warn("failed to send message:", zap.Error(err))
			return err
		}

		return nil
	})
}

func (m *DataHandler) ReplicateMessage(ctx context.Context, param *api.ReplicateMessageParam) error {
	for i, msgBytes := range param.MsgsBytes {
		header := &commonpb.MsgHeader{}
		err := proto.Unmarshal(msgBytes, header)
		if err != nil {
			log.Warn("failed to unmarshal msg header", zap.Int("index", i), zap.Error(err))
			return err
		}

		if header.GetBase() == nil {
			log.Warn("msg header base is nil", zap.Int("index", i), zap.Error(err))
			return err
		}

		err = m.ReplicaMessageHandler(ctx, param)
		if err != nil {
			log.Warn("failed to replca message handle", zap.Int("index", i), zap.Error(err))
			return err
		}
	}

	return nil
}

func (m *DataHandler) DescribeCollection(ctx context.Context, param *api.DescribeCollectionParam) error {
	messageJSON, err := json.Marshal(param)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	return m.DBOp(ctx, func(dbClient net.Conn) error {
		_, err := dbClient.Write(messageJSON)
		if err != nil {
			log.Warn("failed to send message:", zap.Error(err))
			return err
		}

		return nil
	})
}

func (m *DataHandler) DescribeDatabase(ctx context.Context, param *api.DescribeDatabaseParam) error {
	messageJSON, err := json.Marshal(param)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	return m.DBOp(ctx, func(dbClient net.Conn) error {
		_, err := dbClient.Write(messageJSON)
		if err != nil {
			log.Warn("failed to send message:", zap.Error(err))
			return err
		}

		return nil
	})
}

func join(elems []string, sep string) string {
	switch len(elems) {
	case 0:
		return ""
	case 1:
		return elems[0]
	}
	n := len(sep) * (len(elems) - 1)
	for i := 0; i < len(elems); i++ {
		n += len(elems[i])
	}

	var b strings.Builder
	b.Grow(n)
	b.WriteString(elems[0])
	for _, s := range elems[1:] {
		b.WriteString(sep)
		b.WriteString(s)
	}
	return b.String()
}
