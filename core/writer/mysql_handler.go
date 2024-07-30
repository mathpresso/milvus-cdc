package writer

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-sql-driver/mysql"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"strconv"
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
		log.Error("failed to create mysql connection", zap.Error(err))
		return nil, err
	}

	return handler, nil
}

func (m *MySQLDataHandler) createDBConnection(connectionTimeout int) (*sql.DB, error) {
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

func (m *MySQLDataHandler) mysqlOp(ctx context.Context, query string, args ...interface{}) error {
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

	return m.mysqlOp(ctx, query)

}

func interfaceSliceToStringSlice(input []interface{}) []string {
	var output []string
	for _, v := range input {
		str, _ := v.(string)
		output = append(output, str)
	}
	return output
}

func float32SliceToStringSlice(input []float32) []string {
	output := make([]string, len(input))
	for i, v := range input {
		output[i] = strconv.FormatFloat(float64(v), 'f', -1, 32)
	}
	return output
}

func (m *MySQLDataHandler) Delete(ctx context.Context, param *api.DeleteParam) error {
	query := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `%s` = ?", param.Database, param.CollectionName, param.Column.Name)
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
	query := fmt.Sprintf("CREATE INDEX %s ON `%s`.`%s` (`%s`)",
		param.GetIndexName(), param.DbName, param.GetCollectionName(), param.GetFieldName())
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
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", param.DbName)
	log.Info("CREATE DATABASE", zap.String("query", query))
	return m.mysqlOp(ctx, query)
}

func (m *MySQLDataHandler) DropDatabase(ctx context.Context, param *api.DropDatabaseParam) error {
	query := fmt.Sprintf("DROP DATABASE IF EXISTS %s", param.DbName)
	log.Info("DROP DATABASE", zap.String("query", query))
	return m.mysqlOp(ctx, query)
}

func (m *MySQLDataHandler) unmarshalTsMsg(ctx context.Context, msgType commonpb.MsgType, dbName string, msgBytes []byte) error {
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

		log.Info("insert msg", zap.Any("insertMsg", insertMsg.InsertRequest), zap.Any("insertMsgRows", insertMsg.NumRows))
		log.Info("msgBytes", zap.Any("msgBytes", msgBytes))
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

		param.Database = strings.Split(param.ChannelName, "-")[0]
		err = m.unmarshalTsMsg(ctx, header.GetBase().GetMsgType(), param.Database, msgBytes)
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
