package writer

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-sql-driver/mysql"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
)

type MySQLDataHandler struct {
	api.DataHandler

	address        string
	username       string
	password       string
	connectTimeout int
	db             *sql.DB
	retryOptions   *backoff.ExponentialBackOff
	mysqlCli       *sql.DB
}

func NewMySQLDataHandler(options ...config.Option[*MySQLDataHandler]) (*MySQLDataHandler, error) {
	handler := &MySQLDataHandler{
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
	timeoutContext, cancel := context.WithTimeout(context.Background(), time.Duration(handler.connectTimeout)*time.Second)
	defer cancel()

	handler.db, err = handler.createDBConnection(timeoutContext)
	if err != nil {
		return nil, err
	}

	return handler, nil
}

func (m *MySQLDataHandler) createDBConnection(ctx context.Context) (*sql.DB, error) {
	connector, err := mysql.NewConnector(&mysql.Config{
		Addr:                 m.address,
		User:                 m.username,
		Passwd:               m.password,
		Net:                  "tcp",
		AllowNativePasswords: true,
		DBName:               "information_schema",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create connector: %v", err)
	}

	db := sql.OpenDB(connector)

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}

	return db, nil
}

func (m *MySQLDataHandler) mysqlOp(ctx context.Context, query string, args ...interface{}) error {
	retryFunc := func() error {
		_, err := m.db.ExecContext(ctx, query, args...)
		return err
	}

	err := backoff.Retry(retryFunc, backoff.WithContext(m.retryOptions, ctx))
	if err != nil {
		log.Warn("retry operation failed", zap.Error(err))
	}
	return err
}

func (m *MySQLDataHandler) CreateCollection(ctx context.Context, param *api.CreateCollectionParam) error {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", param.Schema.CollectionName, param.Schema.Fields)
	return m.mysqlOp(ctx, query)
}

func (m *MySQLDataHandler) DropCollection(ctx context.Context, param *api.DropCollectionParam) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", param.CollectionName)
	return m.mysqlOp(ctx, query)
}

func (m *MySQLDataHandler) Insert(ctx context.Context, param *api.InsertParam) error {
	columns := []string{}
	values := []string{}
	for _, col := range param.Columns {
		columns = append(columns, col.Name())
		values = append(values, "?")
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		param.CollectionName, join(columns, ","), join(values, ","))
	args := make([]interface{}, len(param.Columns))
	for i, col := range param.Columns {
		args[i] = col.FieldData()
	}
	return m.mysqlOp(ctx, query, args...)
}

func (m *MySQLDataHandler) Delete(ctx context.Context, param *api.DeleteParam) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", param.CollectionName, param.Column.Name)
	return m.mysqlOp(ctx, query, param.Column.FieldData())
}

func (m *MySQLDataHandler) CreatePartition(ctx context.Context, param *api.CreatePartitionParam) error {
	log.Warn("CreatePartition is not implemented in MySQL, please check it")
	return nil
}

func (m *MySQLDataHandler) DropPartition(ctx context.Context, param *api.DropPartitionParam) error {
	log.Warn("DropPartition is not implemented in MySQL, please check it")
	return nil
}

func (m *MySQLDataHandler) CreateIndex(ctx context.Context, param *api.CreateIndexParam) error {
	query := fmt.Sprintf("CREATE INDEX %s ON %s (%s)",
		param.GetIndexName(), param.GetCollectionName(), param.GetFieldName())
	return m.mysqlOp(ctx, query)
}

func (m *MySQLDataHandler) DropIndex(ctx context.Context, param *api.DropIndexParam) error {
	query := fmt.Sprintf("DROP INDEX %s ON %s", param.IndexName, param.CollectionName)
	return m.mysqlOp(ctx, query)
}

func (m *MySQLDataHandler) LoadCollection(ctx context.Context, param *api.LoadCollectionParam) error {
	log.Warn("LoadCollection is not implemented in MySQL, please check it")
	return nil
}

func (m *MySQLDataHandler) ReleaseCollection(ctx context.Context, param *api.ReleaseCollectionParam) error {
	log.Warn("ReleaseCollection is not implemented in MySQL, please check it")
	return nil
}

func (m *MySQLDataHandler) LoadPartitions(ctx context.Context, param *api.LoadPartitionsParam) error {
	log.Warn("LoadPartitions is not implemented in MySQL, please check it")
	return nil
}

func (m *MySQLDataHandler) ReleasePartitions(ctx context.Context, param *api.ReleasePartitionsParam) error {
	log.Warn("ReleasePartitions is not implemented in MySQL, please check it")
	return nil
}
func (m *MySQLDataHandler) Flush(ctx context.Context, param *api.FlushParam) error {
	log.Warn("Flush is not implemented in MySQL, please check it")
	return nil
}

func (m *MySQLDataHandler) CreateDatabase(ctx context.Context, param *api.CreateDatabaseParam) error {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", param.DbName)
	return m.mysqlOp(ctx, query)
}

func (m *MySQLDataHandler) DropDatabase(ctx context.Context, param *api.DropDatabaseParam) error {
	query := fmt.Sprintf("DROP DATABASE IF EXISTS %s", param.DbName)
	return m.mysqlOp(ctx, query)
}

func (m *MySQLDataHandler) ReplicateMessage(ctx context.Context, param *api.ReplicateMessageParam) error {
	var (
		resp  *entity.MessageInfo
		err   error
		opErr error
	)
	opErr = m.mysqlOp(ctx, "", func(mysqlCli client.Client) error {
		resp, err = mysqlCli.ReplicateMessage(ctx, param.ChannelName,
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

func (m *MySQLDataHandler) DescribeCollection(ctx context.Context, param *api.DescribeCollectionParam) error {
	query := fmt.Sprintf("DESCRIBE %s", param.Name)
	return m.mysqlOp(ctx, query)
}

func (m *MySQLDataHandler) DescribeDatabase(ctx context.Context, param *api.DescribeDatabaseParam) error {
	query := "SHOW DATABASES"
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
