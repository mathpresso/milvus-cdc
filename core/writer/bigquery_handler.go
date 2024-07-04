package writer

import (
	"context"
	"fmt"
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

type BigQueryDataHandler struct {
	api.DataHandler

	projectID      string
	credentials    string
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
	if handler.credentials == "" {
		return nil, errors.New("empty BigQuery credentials")
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

func (m *BigQueryDataHandler) createBigQueryClient(ctx context.Context) (*bigquery.Client, error) {
	client, err := bigquery.NewClient(ctx, m.projectID, option.WithCredentialsFile(m.credentials))
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}
	return client, nil
}

func (m *BigQueryDataHandler) bigqueryOp(ctx context.Context, query string, params map[string]interface{}) error {
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

func (m *BigQueryDataHandler) CreateTable(ctx context.Context, param *api.CreateCollectionParam) error {
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

func (m *BigQueryDataHandler) DropTable(ctx context.Context, param *api.DropCollectionParam) error {
	tableRef := m.client.Dataset(param.Database).Table(param.CollectionName)
	return tableRef.Delete(ctx)
}

func (m *BigQueryDataHandler) Insert(ctx context.Context, param *api.InsertParam) error {
	inserter := m.client.Dataset(param.Database).Table(param.CollectionName).Inserter()
	var rows []*bigquery.ValuesSaver
	var colName bigquery.Schema
	var colValues []bigquery.Value

	for i, col := range param.Columns {
		colName[i].Name = col.Name()
		colValues[i] = col.FieldData()
	}

	row := &bigquery.ValuesSaver{
		Schema: colName,
		Row:    colValues,
	}
	rows = append(rows, row)

	return inserter.Put(ctx, rows)
}

func (m *BigQueryDataHandler) Delete(ctx context.Context, param *api.DeleteParam) error {
	query := fmt.Sprintf("DELETE FROM `%s.%s` WHERE %s = @value", param.Database, param.CollectionName, param.Column.Name())
	params := map[string]interface{}{"value": param.Column.FieldData()}
	return m.bigqueryOp(ctx, query, params)
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

func (m *BigQueryDataHandler) DescribeTable(ctx context.Context, param *api.DescribeCollectionParam) error {
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
