package writer

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-sql-driver/mysql"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
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

	handler.db, err = handler.createDBConnection(handler.connectTimeout)
	if err != nil {
		return nil, err
	}

	return handler, nil
}

func (m *MySQLDataHandler) createDBConnection(connectionTimeout int) (*sql.DB, error) {
	connector, err := mysql.NewConnector(&mysql.Config{
		Addr:                 m.address,
		User:                 m.username,
		Passwd:               m.password,
		Net:                  "tcp",
		AllowNativePasswords: true,
		DBName:               "information_schema",
		Timeout:              time.Duration(connectionTimeout) * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create connector: %v", err)
	}

	db := sql.OpenDB(connector)
	/*
		if err := db.Ping(); err != nil {
			return nil, fmt.Errorf("failed to ping mysql database : %v", err)
		}
	*/
	return db, nil
}

func (m *MySQLDataHandler) mysqlOp(ctx context.Context, query string, args ...interface{}) error {
	retryFunc := func() error {
		log.Info("executing mysql operation", zap.String("query", query), zap.Any("args", args))
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
	log.Info("CREATE TABLE", zap.String("query", query))
	return m.mysqlOp(ctx, query)
}

func (m *MySQLDataHandler) DropCollection(ctx context.Context, param *api.DropCollectionParam) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", param.CollectionName)
	log.Info("DROP TABLE", zap.String("query", query))
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
	log.Info("INSERT", zap.String("query", query))
	args := make([]interface{}, len(param.Columns))
	for i, col := range param.Columns {
		args[i] = col.FieldData()
	}
	return m.mysqlOp(ctx, query, args...)
}

func (m *MySQLDataHandler) Delete(ctx context.Context, param *api.DeleteParam) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", param.CollectionName, param.Column.Name)
	log.Info("DELETE", zap.String("query", query))
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
	log.Info("CREATE INDEX", zap.String("query", query))
	return m.mysqlOp(ctx, query)
}

func (m *MySQLDataHandler) DropIndex(ctx context.Context, param *api.DropIndexParam) error {
	query := fmt.Sprintf("DROP INDEX %s ON %s", param.IndexName, param.CollectionName)
	log.Info("DROP INDEX", zap.String("query", query))
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
	log.Info("CREATE DATABASE", zap.String("query", query))
	return m.mysqlOp(ctx, query)
}

func (m *MySQLDataHandler) DropDatabase(ctx context.Context, param *api.DropDatabaseParam) error {
	query := fmt.Sprintf("DROP DATABASE IF EXISTS %s", param.DbName)
	log.Info("DROP DATABASE", zap.String("query", query))
	return m.mysqlOp(ctx, query)
}

func (m *MySQLDataHandler) unmarshalTsMsg(ctx context.Context, msgType commonpb.MsgType, msgBytes []byte) error {
	//var tsMsg msgstream.TsMsg
	var err error

	switch msgType {
	case commonpb.MsgType_Insert:
		insertMsg := &msgstream.InsertMsg{}
		err = proto.Unmarshal(msgBytes, insertMsg)
		if err != nil {
			log.Warn("failed to unmarshal insert msg", zap.Error(err))
			return err
		}

		msg, err := convertInsertMsgToInsertParam(insertMsg)
		if err != nil {
			log.Warn("failed to convert insert msg to insert param", zap.Error(err))
			return err
		}

		err = m.Insert(ctx, msg)
		if err != nil {
			log.Warn("failed to unmarshal insert msg", zap.Error(err))
			return err
		}
	case commonpb.MsgType_Delete:
		deleteMsg := &msgstream.DeleteMsg{}
		err = proto.Unmarshal(msgBytes, deleteMsg)

		msg, err := convertDeleteMsgToDeleteParam(deleteMsg)
		if err != nil {
			log.Warn("failed to convert delete msg to delete param", zap.Error(err))
			return err
		}
		err = m.Delete(ctx, msg)
		if err != nil {
			log.Warn("failed to delete", zap.Error(err))
			return err
		}
	case commonpb.MsgType_Upsert:
		// UpsertMsg는 InsertMsg와 DeleteMsg를 포함하므로, 따로 언마샬링한 후 조립합니다.
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

func (m *MySQLDataHandler) ReplicateMessage(ctx context.Context, param *api.ReplicateMessageParam) error {
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

		err = m.unmarshalTsMsg(ctx, header.GetBase().GetMsgType(), msgBytes)
		if err != nil {
			log.Warn("failed to unmarshal msg", zap.Int("index", i), zap.Error(err))
			return err
		}

	}

	return nil
}

func (m *MySQLDataHandler) DescribeCollection(ctx context.Context, param *api.DescribeCollectionParam) error {
	query := fmt.Sprintf("DESCRIBE %s", param.Name)
	log.Info("DESCRIBE", zap.String("query", query))
	return m.mysqlOp(ctx, query)
}

func (m *MySQLDataHandler) DescribeDatabase(ctx context.Context, param *api.DescribeDatabaseParam) error {
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
