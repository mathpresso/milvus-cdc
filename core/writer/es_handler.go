package writer

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"time"

	"github.com/cenkalti/backoff/v4"
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
	db             *sql.DB
	retryOptions   *backoff.ExponentialBackOff
	mysqlCli       *sql.DB
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
	if handler.address == "" {
		return nil, errors.New("empty MySQL address")
	}

	var err error

	handler.db, err = handler.createESClient(handler.connectTimeout)
	if err != nil {
		log.Error("failed to create mysql connection", zap.Error(err))
		return nil, err
	}

	return handler, nil
}

func (m *ESDataHandler) createESClient(connectionTimeout int) (*sql.DB, error) {
	cfg := mysql.Config{
		Addr:                 m.address,
		User:                 m.username,
		Passwd:               m.password,
		Net:                  "tcp",
		AllowNativePasswords: true,
		Timeout:              time.Duration(connectionTimeout) * time.Second,
	}

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		log.Info("failed to connect mysql database", zap.Any("info", cfg.FormatDSN()))
		return nil, fmt.Errorf("failed to connect mysql database : %v", err)
	}

	return db, nil

}

func (m *ESDataHandler) ESOp(ctx context.Context, query string, args ...interface{}) error {
	retryFunc := func() error {
		log.Info("executing mysql operation", zap.String("query", query), zap.Any("args", args))
		_, err := m.db.Exec(query)
		if err != nil {
			log.Warn("failed to execute mysql operation", zap.Error(err))
		}
		return err
	}

	err := backoff.Retry(retryFunc, backoff.WithContext(m.retryOptions, ctx))
	if err != nil {
		log.Warn("retry operation failed", zap.Error(err))
	}
	return err
}

func (m *ESDataHandler) CreateCollection(ctx context.Context, param *api.CreateCollectionParam) error {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", param.Schema.CollectionName, param.Schema.Fields)
	log.Info("CREATE TABLE", zap.String("query", query))
	return m.ESOp(ctx, query)
}

func (m *ESDataHandler) DropCollection(ctx context.Context, param *api.DropCollectionParam) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", param.CollectionName)
	log.Info("DROP TABLE", zap.String("query", query))
	return m.ESOp(ctx, query)
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
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", param.DbName)
	log.Info("CREATE DATABASE", zap.String("query", query))
	return m.ESOp(ctx, query)
}

func (m *ESDataHandler) DropDatabase(ctx context.Context, param *api.DropDatabaseParam) error {
	query := fmt.Sprintf("DROP DATABASE IF EXISTS %s", param.DbName)
	log.Info("DROP DATABASE", zap.String("query", query))
	return m.ESOp(ctx, query)
}

func (m *ESDataHandler) DescribeCollection(ctx context.Context, param *api.DescribeCollectionParam) error {
	query := fmt.Sprintf("DESCRIBE %s", param.Name)
	log.Info("DESCRIBE", zap.String("query", query))
	return m.ESOp(ctx, query)
}

func (m *ESDataHandler) DescribeDatabase(ctx context.Context, param *api.DescribeDatabaseParam) error {
	query := "SHOW DATABASES"
	log.Info(query)
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var dbName string
	for rows.Next() {
		err := rows.Scan(&dbName)
		if err != nil {
			return err
		}
		if dbName == param.Name {
			return nil
		}
	}
	return errors.Newf("database [%s] not found", param.Name)
}
