package writer

import (
	"context"
	"fmt"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"go.uber.org/zap"
)

type BigQueryDataHandler struct {
	api.DataHandler

	projectID string
	//	credentials    string
	connectTimeout int
	client         *bigquery.Client
	retryOptions   *backoff.ExponentialBackOff
}

// NewBigQueryDataHandler options must include ProjectIDOption and CredentialsOption
func NewBigQueryDataHandler(options ...config.Option[*BigQueryDataHandler]) (*BigQueryDataHandler, error) {
	handler := &BigQueryDataHandler{
		connectTimeout: 5,
		retryOptions:   backoff.NewExponentialBackOff(),
	}
	handler.retryOptions.MaxElapsedTime = 2 * time.Minute // 설정된 최대 재시도 시간

	for _, option := range options {
		option.Apply(handler)
	}
	if handler.projectID == "" {
		return nil, errors.New("empty BigQuery project ID")
	}

	var err error
	timeoutContext, cancel := context.WithTimeout(context.Background(), time.Duration(handler.connectTimeout)*time.Second)
	defer cancel()

	handler.client, err = handler.createBigQueryClient(timeoutContext)
	if err != nil {
		return nil, err
	}

	return handler, nil
}

/*
func (m *BigQueryDataHandler) createBigQueryClient(ctx context.Context) (*bigquery.Client, error) {
	// TLS 인증서 검증을 비활성화하는 HTTP 클라이언트 설정
	insecureTransport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: false,
		},
	}
	httpClient := &http.Client{
		Transport: insecureTransport,
	}

	// 클라이언트 옵션 설정
	clientOptions := []option.ClientOption{
		option.WithHTTPClient(httpClient),
	}

	// BigQuery 클라이언트 생성
	client, err := bigquery.NewClient(ctx, m.projectID, clientOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}

	return client, nil
}
*/

func (m *BigQueryDataHandler) createBigQueryClient(ctx context.Context) (*bigquery.Client, error) {
	client, err := bigquery.NewClient(ctx, m.projectID)
	if err != nil {
		log.Warn("failed to create BigQuery client", zap.Error(err))
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}
	return client, nil
}

func (m *BigQueryDataHandler) bigqueryOp(ctx context.Context, query string, params map[string]interface{}) error {
	retryFunc := func() error {
		q := m.client.Query(query)

		job, err := q.Run(ctx)
		if err != nil {
			log.Warn("failed to run query", zap.Error(err))
			return err
		}
		status, err := job.Wait(ctx)
		if err != nil {
			return err
		}
		if err := status.Err(); err != nil {
			return err
		}
		return nil
	}

	err := backoff.Retry(retryFunc, backoff.WithContext(m.retryOptions, ctx))
	if err != nil {
		log.Warn("retry operation failed", zap.Error(err))
	}
	return err
}

func (m *BigQueryDataHandler) CreateCollection(ctx context.Context, param *api.CreateCollectionParam) error {
	schema := bigquery.Schema{}
	for _, field := range param.Schema.Fields {
		schema = append(schema, &bigquery.FieldSchema{
			Name: field.Name,
			Type: bigquery.FieldType(field.DataType),
		})
	}
	metaData := &bigquery.TableMetadata{
		Schema: schema,
	}
	tableRef := m.client.Dataset(param.Database).Table(param.Schema.CollectionName)
	return tableRef.Create(ctx, metaData)
}

func (m *BigQueryDataHandler) DropCollection(ctx context.Context, param *api.DropCollectionParam) error {
	tableRef := m.client.Dataset(param.Database).Table(param.CollectionName)
	return tableRef.Delete(ctx)
}

func (m *BigQueryDataHandler) Insert(ctx context.Context, param *api.InsertParam) error {
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

	var value, values string

	for rowCnt := 0; rowCnt < len(rowValues[0]); rowCnt++ {
		for colNo, _ := range columns {
			switch rowValues[colNo][rowCnt].(type) {
			case string:
				if colNo == 0 {
					value = fmt.Sprintf("(%s", fmt.Sprintf("%s", rowValues[colNo][rowCnt]))
				} else {
					value = fmt.Sprintf("%s,%s", value, fmt.Sprintf("%s", rowValues[colNo][rowCnt]))
				}
			default:
				if colNo == 0 {
					value = fmt.Sprintf("(%s", fmt.Sprintf("'%v'", rowValues[colNo][rowCnt]))
				} else {
					value = fmt.Sprintf("%s,%s", value, fmt.Sprintf("'%v'", rowValues[colNo][rowCnt]))
				}
			}
		}
		value = fmt.Sprintf("%s)", value)

		if rowCnt == 0 {
			values = fmt.Sprintf("%s", value)
		} else {
			values = fmt.Sprintf("%s,%s", values, value)
		}
	}

	//string_to_vector('[1,2,3]')
	query := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s",
		param.Database, param.CollectionName, join(columns, ","), values)
	log.Info("INSERT", zap.String("query", query))

	return m.bigqueryOp(ctx, query, nil)
}

func (m *BigQueryDataHandler) Delete(ctx context.Context, param *api.DeleteParam) error {
	query := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `%s` = @value", param.Database, param.CollectionName, param.Column.Name())
	//params := map[string]interface{}{"value": param.Column.FieldData()}
	return m.bigqueryOp(ctx, query, nil)
}

func (m *BigQueryDataHandler) CreateIndex(ctx context.Context, param *api.CreateIndexParam) error {
	// BigQuery는 Vector Search Index만을 지원합니다.
	query := fmt.Sprintf("CREATE OR REPLACE VECTOR INDEX `%s`.%s ON `%s`(embedding_v1) OPTIONS(distance_type='L2',index_type_type='IVF')", param.Database, param.IndexName, param.CollectionName)
	return m.bigqueryOp(ctx, query, nil)
}

func (m *BigQueryDataHandler) DropIndex(ctx context.Context, param *api.DropIndexParam) error {
	// BigQuery는 인덱스를 지원하지 않습니다.
	return nil
}

func (m *BigQueryDataHandler) CreateDatabase(ctx context.Context, param *api.CreateDatabaseParam) error {
	// BigQuery에서는 데이터셋을 생성하는 방식입니다.
	dataset := m.client.Dataset(param.DbName)
	return dataset.Create(ctx, &bigquery.DatasetMetadata{})
}

func (m *BigQueryDataHandler) DropDatabase(ctx context.Context, param *api.DropDatabaseParam) error {
	// BigQuery에서는 데이터셋을 삭제하는 방식입니다.
	dataset := m.client.Dataset(param.DbName)
	return dataset.Delete(ctx)
}

func (m *BigQueryDataHandler) unmarshalTsMsg(ctx context.Context, msgType commonpb.MsgType, dbName string, msgBytes []byte) error {
	var tsMsg msgstream.TsMsg
	var err error

	if msgBytes == nil {
		log.Warn("msgBytes is nil")
		return errors.New("msgBytes is nil")
	}

	switch msgType {
	case commonpb.MsgType_Insert:
		tsMsg = &msgstream.InsertMsg{}

		msg, err := tsMsg.Unmarshal(msgBytes)
		if err != nil {
			log.Warn("failed to unmarshal ts msg", zap.Error(err))
			return err
		}

		log.Info("unmarshalTsMsg", zap.Any("msgType", msgType), zap.Any("msg", msg))

		insertMsg := msg.(*msgstream.InsertMsg)

		tmsg, err := convertInsertMsgToInsertParam(insertMsg)
		if err != nil {
			log.Warn("failed to convert insert msg to insert param", zap.Error(err))
			return err
		}

		tmsg.Database = dbName
		err = m.Insert(ctx, tmsg)
		if err != nil {
			log.Warn("failed to unmarshal insert msg", zap.Error(err))
			return err
		}
	case commonpb.MsgType_Delete:
		tsMsg = &msgstream.DeleteMsg{}

		msg, err := tsMsg.Unmarshal(msgBytes)
		if err != nil {
			log.Warn("failed to unmarshal ts msg", zap.Error(err))
			return err
		}

		log.Info("unmarshalTsMsg", zap.Any("msgType", msgType), zap.Any("msg", msg))

		deleteMsg := msg.(*msgstream.DeleteMsg)

		tmsg, err := convertDeleteMsgToDeleteParam(deleteMsg)
		if err != nil {
			log.Warn("failed to convert delete msg to delete param", zap.Error(err))
			return err
		}

		tmsg.Database = dbName

		err = m.Delete(ctx, tmsg)
		if err != nil {
			log.Warn("failed to delete", zap.Error(err))
			return err
		}
	case commonpb.MsgType_Upsert:
		// UpsertMsg는 InsertMsg와 DeleteMsg를 포함하므로, 따로 언마샬링한 후 조립합니다.
		// tsMsg = &msgstream.UpsertMsg{}
		insertMsg := &msgstream.InsertMsg{}
		deleteMsg := &msgstream.DeleteMsg{}

		// msgBytes를 분할하여 각 메시지에 언마샬링합니다. (여기서는 단순히 msgBytes를 나눠서 가정합니다.)
		half := len(msgBytes) / 2
		err = proto.Unmarshal(msgBytes[:half], insertMsg)
		if err != nil {
			log.Warn("failed to unmarshal insert msg", zap.Error(err))
			return err
		}

		imsg, err := convertInsertMsgToInsertParam(insertMsg)
		if err != nil {
			log.Warn("failed to convert insert msg to insert param", zap.Error(err))
			return err
		}

		err = m.Insert(ctx, imsg)
		if err != nil {
			log.Warn("failed to insert", zap.Error(err))
			return err
		}

		err = proto.Unmarshal(msgBytes[half:], deleteMsg)
		if err != nil {
			return err
		}

		dmsg, err := convertDeleteMsgToDeleteParam(deleteMsg)
		if err != nil {
			log.Warn("failed to convert delete msg to delete param", zap.Error(err))
			return err
		}

		err = m.Delete(ctx, dmsg)
		if err != nil {
			log.Warn("failed to delete", zap.Error(err))
			return err
		}
	case commonpb.MsgType_TimeTick:
		return nil
	default:
		log.Warn("unsupported message type", zap.Any("msgType", msgType))
		err = fmt.Errorf("unsupported message type: %v", msgType)
		return err
	}

	return nil
}

func (m *BigQueryDataHandler) ReplicateMessage(ctx context.Context, param *api.ReplicateMessageParam) error {
	for i, msgBytes := range param.MsgsBytes {
		header := &commonpb.MsgHeader{}
		err := proto.Unmarshal(msgBytes, header)
		if err != nil {
			log.Warn("failed to unmarshal msg header", zap.Int("index", i), zap.Error(err))
			return err
		}

		if header.GetBase() == nil {
			log.Warn("msg header base is nil", zap.Int("index", i))
			return err
		}

		param.Database = strings.Split(param.ChannelName, "-")[0]
		err = m.unmarshalTsMsg(ctx, header.GetBase().GetMsgType(), param.Database, msgBytes)
		if err != nil {
			log.Warn("failed to unmarshal msg", zap.Int("index", i), zap.Error(err))
			return err
		}

	}

	return nil
}

func (m *BigQueryDataHandler) DescribeCollection(ctx context.Context, param *api.DescribeCollectionParam) error {
	tableRef := m.client.Dataset(param.Database).Table(param.Name)
	metaData, err := tableRef.Metadata(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Table: %s, Schema: %v\n", param.Name, metaData.Schema)
	return nil
}

func (m *BigQueryDataHandler) DescribeDatabase(ctx context.Context, param *api.DescribeDatabaseParam) error {
	dataset := m.client.Dataset(param.Name)
	metaData, err := dataset.Metadata(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Dataset: %s, Location: %s\n", param.Name, metaData.Location)
	return nil
}
