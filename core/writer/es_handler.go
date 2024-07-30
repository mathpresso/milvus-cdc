package writer

import (
	"context"
	"fmt"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/api/option"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
)

type ESDataHandler struct {
	api.DataHandler

	projectID      string
	credentials    string
	connectTimeout int
	client         *bigquery.Client
	retryOptions   *backoff.ExponentialBackOff
}

// NewBigQueryDataHandler options must include ProjectIDOption and CredentialsOption
func NewESDataHandler(options ...config.Option[*ESDataHandler]) (*ESDataHandler, error) {
	handler := &ESDataHandler{
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
	if handler.credentials == "" {
		return nil, errors.New("empty BigQuery credentials")
	}

	var err error
	timeoutContext, cancel := context.WithTimeout(context.Background(), time.Duration(handler.connectTimeout)*time.Second)
	defer cancel()

	handler.client, err = handler.createESClient(timeoutContext)
	if err != nil {
		return nil, err
	}

	return handler, nil
}

func (m *ESDataHandler) createESClient(ctx context.Context) (*bigquery.Client, error) {
	client, err := bigquery.NewClient(ctx, m.projectID, option.WithCredentialsFile(m.credentials))
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}
	return client, nil
}

func (m *ESDataHandler) ESOp(ctx context.Context, query string, params map[string]interface{}) error {
	retryFunc := func() error {
		q := m.client.Query(query)
		q.Parameters = make([]bigquery.QueryParameter, 0, len(params))
		for k, v := range params {
			q.Parameters = append(q.Parameters, bigquery.QueryParameter{Name: k, Value: v})
		}
		job, err := q.Run(ctx)
		if err != nil {
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

func (m *ESDataHandler) CreateTable(ctx context.Context, param *api.CreateCollectionParam) error {
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

func (m *ESDataHandler) DropTable(ctx context.Context, param *api.DropCollectionParam) error {
	tableRef := m.client.Dataset(param.Database).Table(param.CollectionName)
	return tableRef.Delete(ctx)
}

func (m *ESDataHandler) Insert(ctx context.Context, param *api.InsertParam) error {
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
						colValues = append(colValues, fmt.Sprintf("string_to_vector('[%v]')", join(float32SliceToStringSlice(vec), ",")))
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
	//	log.Info("INSERT", zap.String("query", query))

	return m.ESOp(ctx, query, nil)
}

func (m *ESDataHandler) Delete(ctx context.Context, param *api.DeleteParam) error {
	query := fmt.Sprintf("DELETE FROM `%s.%s` WHERE %s = @value", param.Database, param.CollectionName, param.Column.Name())
	params := map[string]interface{}{"value": param.Column.FieldData()}
	return m.ESOp(ctx, query, params)
}

func (m *ESDataHandler) CreateIndex(ctx context.Context, param *api.CreateIndexParam) error {
	// BigQuery는 Vector Search Index만을 지원합니다.
	query := fmt.Sprintf("CREATE OR REPLACE VECTOR INDEX `%s`.%s ON `%s`(embedding_v1) OPTIONS(distance_type='L2',index_type_type='IVF')", param.Database, param.IndexName, param.CollectionName)
	return m.ESOp(ctx, query, nil)
}

func (m *ESDataHandler) DropIndex(ctx context.Context, param *api.DropIndexParam) error {
	// BigQuery는 인덱스를 지원하지 않습니다.
	return nil
}

func (m *ESDataHandler) CreateDatabase(ctx context.Context, param *api.CreateDatabaseParam) error {
	// BigQuery에서는 데이터셋을 생성하는 방식입니다.
	dataset := m.client.Dataset(param.DbName)
	return dataset.Create(ctx, &bigquery.DatasetMetadata{})
}

func (m *ESDataHandler) DropDatabase(ctx context.Context, param *api.DropDatabaseParam) error {
	// BigQuery에서는 데이터셋을 삭제하는 방식입니다.
	dataset := m.client.Dataset(param.DbName)
	return dataset.Delete(ctx)
}

func (m *ESDataHandler) DescribeTable(ctx context.Context, param *api.DescribeCollectionParam) error {
	tableRef := m.client.Dataset(param.Database).Table(param.Name)
	metaData, err := tableRef.Metadata(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Table: %s, Schema: %v\n", param.Name, metaData.Schema)
	return nil
}

func (m *ESDataHandler) DescribeDatabase(ctx context.Context, param *api.DescribeDatabaseParam) error {
	dataset := m.client.Dataset(param.Name)
	metaData, err := dataset.Metadata(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Dataset: %s, Location: %s\n", param.Name, metaData.Location)
	return nil
}
