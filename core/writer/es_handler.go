package writer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/zilliztech/milvus-cdc/core/util"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"go.uber.org/zap"
)

type ESDataHandler struct {
	api.DataHandler

	address        string
	username       string
	password       string
	connectTimeout int
	retryOptions   []retry.Option
	esClient       *elasticsearch.Client
}

func NewESDataHandler(options ...config.Option[*ESDataHandler]) (*ESDataHandler, error) {
	handler := &ESDataHandler{
		connectTimeout: 5,
	}

	handler.retryOptions = util.GetRetryOptions(config.GetCommonConfig().Retry)

	for _, option := range options {
		option.Apply(handler)
	}
	if handler.address == "" {
		return nil, errors.New("empty MySQL address")
	}

	if handler.username == "" || handler.password == "" {
		return nil, errors.New("empty MySQL username or password")
	}

	var err error
	timeoutContext, cancel := context.WithTimeout(context.Background(), time.Duration(handler.connectTimeout)*time.Second)
	defer cancel()

	err = handler.ESOp(timeoutContext, func(esClient *elasticsearch.Client) error {
		return nil
	})
	if err != nil {
		log.Warn("fail to new the elastic search client", zap.Error(err))
		return nil, err
	}
	handler.retryOptions = util.GetRetryOptions(config.GetCommonConfig().Retry)

	return handler, nil
}

func (m *ESDataHandler) ESOp(ctx context.Context, f func(esClient *elasticsearch.Client) error) error {
	retryMySqlFunc := func(c *elasticsearch.Client) error {
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

	esClient, err := util.GetMySqlClientManager().GetESClient(ctx, m.address, m.username, m.password, "", false, m.connectTimeout)
	if err != nil {
		log.Warn("fail to get elastic search client", zap.Error(err))
		return err
	}

	return retryMySqlFunc(esClient)
}

func (m *ESDataHandler) CreateCollection(ctx context.Context, param *api.CreateCollectionParam) error {
	return m.ESOp(ctx, func(es *elasticsearch.Client) error {
		_, err := es.Indices.Create(param.Schema.CollectionName)
		return err
	})
}

func (m *ESDataHandler) DropCollection(ctx context.Context, param *api.DropCollectionParam) error {
	return m.ESOp(ctx, func(es *elasticsearch.Client) error {
		_, err := es.Indices.Delete([]string{param.CollectionName})
		return err
	})

}

func insertDocument(ctx context.Context, es *elasticsearch.Client, indexName string, docs []map[string]interface{}) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(docs); err != nil {
		return fmt.Errorf("Error encoding document: %v", err)
	}

	req := esapi.IndexRequest{
		Index: indexName,
		Body:  &buf,
		//		DocumentID: fmt.Sprintf("%v", doc["id"]),
		Refresh: "true", // Refresh the index after performing the request
	}

	res, err := req.Do(ctx, es)
	if err != nil {
		return fmt.Errorf("Error indexing document: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Error indexing document Name=%s: %s", indexName, res.String())
	}

	log.Info("Document inserted successfully")

	return nil
}

func (m *ESDataHandler) Insert(ctx context.Context, param *api.InsertParam) error {
	var columns []string

	var rowValues [][]interface{}
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
						colValues = append(colValues, vec)
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

	var docs []map[string]interface{}

	doc := make(map[string]interface{})

	for rowCnt := 0; rowCnt < len(rowValues[0]); rowCnt++ {
		for colNo, colName := range columns {
			switch rowValues[colNo][rowCnt].(type) {
			case string:
				doc[colName] = fmt.Sprintf("'%v'", rowValues[colNo][rowCnt])
			default:
				doc[colName] = rowValues[colNo][rowCnt]
			}
		}
		docs = append(docs, doc)
	}

	return m.ESOp(ctx, func(es *elasticsearch.Client) error {
		return insertDocument(ctx, es, param.CollectionName, docs)
	})
}

func deleteDocumentsByQuery(ctx context.Context, es *elasticsearch.Client, index string, query map[string]interface{}) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return fmt.Errorf("Error encoding query: %v", err)
	}

	// Delete By Query 요청 생성
	req := esapi.DeleteByQueryRequest{
		Index: []string{index},
		Body:  &buf,
	}

	// 요청 실행
	res, err := req.Do(ctx, es)
	if err != nil {
		return fmt.Errorf("Error executing request: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Error response from server: %s", res.String())
	}

	log.Info("Documents deleted successfully")
	return nil
}

func (m *ESDataHandler) Delete(ctx context.Context, param *api.DeleteParam) error {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				param.Column.Name(): param.Column.FieldData(),
			},
		},
	}

	res, err := m.esClient.Delete(param.CollectionName, param.Column.FieldData().String())
	if err != nil {
		return fmt.Errorf("Error getting response: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Error deleting document ID=%s: %s", param.Column.FieldData().String(), res.String())
	}

	log.Info("Document ID=%s deleted successfully", zap.String("doc-id", param.Column.FieldData().String()))

	return m.ESOp(ctx, func(es *elasticsearch.Client) error {
		return deleteDocumentsByQuery(ctx, es, param.CollectionName, query)
	})
}

func (m *ESDataHandler) CreateIndex(ctx context.Context, param *api.CreateIndexParam) error {
	log.Warn("CreateIndex is not implemented in ElasticSearch, please check it")
	return nil
}

func (m *ESDataHandler) DropIndex(ctx context.Context, param *api.DropIndexParam) error {
	// BigQuery는 인덱스를 지원하지 않습니다.
	log.Warn("DropIndex is not implemented in ElasticSearch, please check it")
	return nil
}

func (m *ESDataHandler) CreateDatabase(ctx context.Context, param *api.CreateDatabaseParam) error {
	log.Warn("CreateDatabase is not implemented in ElasticSearch, please check it")
	return nil
}

func (m *ESDataHandler) DropDatabase(ctx context.Context, param *api.DropDatabaseParam) error {
	log.Warn("DropDatabase is not implemented in ElasticSearch, please check it")
	return nil
}

func (m *ESDataHandler) DescribeCollection(ctx context.Context, param *api.DescribeCollectionParam) error {
	log.Warn("DescribeCollection is not implemented in ElasticSearch, please check it")
	return nil
}

func (m *ESDataHandler) DescribeDatabase(ctx context.Context, param *api.DescribeDatabaseParam) error {
	log.Warn("DescribeDatabase is not implemented in ElasticSearch, please check it")
	return nil
}
