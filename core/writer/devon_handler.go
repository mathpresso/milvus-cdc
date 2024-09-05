package writer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/util/resource"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	pb "github.com/zilliztech/milvus-cdc/core/grpc/receive_msg"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/util"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strconv"
	"strings"
	"time"
)

type DataHandler struct {
	api.DataHandler

	targetDBType    string          `json:"target_db_type"`
	agentHost       string          `json:"agent_host"`
	agentPort       int             `json:"agent_port"`
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

type Server struct {
	pb.UnimplementedHandleReceiveMsgServiceServer
	DBClient pb.HandleReceiveMsgServiceClient // 다른 서버에 요청을 보내는 클라이언트
	DataHandler
}

func NewDataHandler(options ...config.Option[*Server]) (*Server, error) {
	handler := &Server{
		DataHandler: DataHandler{
			connectTimeout: 5,
		},
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

	err = handler.DBOp(timeoutContext, func(db *grpc.ClientConn) error {
		return nil
	})
	if err != nil {
		log.Warn("fail to new the cdc agent client", zap.Error(err))
		return nil, err
	}
	handler.retryOptions = util.GetRetryOptions(config.GetCommonConfig().Retry)
	return handler, nil
}

func (m *Server) DBOp(ctx context.Context, f func(db *grpc.ClientConn) error) error {
	retryDBAgentFunc := func(c *grpc.ClientConn) error {
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

	dbClient, err := util.GetDBClientManager().GetDBClient(ctx, m.agentHost, m.agentPort, m.uri, m.database, m.collection, util.DialConfig{}, m.connectTimeout)
	if err != nil {
		log.Warn("fail to get cdc agent client", zap.Error(err))
		return err
	}

	return retryDBAgentFunc(dbClient)
}

func (m *Server) ReplicaMessageHandler(ctx context.Context, param *api.ReplicateMessageParam) error {
	var cnt int
	timeTickMsg := true

	if param.ChannelName != "" {
		m.database = strings.Split(param.ChannelName, "-")[0]
		m.collection = strings.Split(param.ChannelName, "-")[1]
	}

	m.msgBytes = param.MsgsBytes
	for i, msgBytes := range m.msgBytes {
		header := &commonpb.MsgHeader{}
		err := proto.Unmarshal(msgBytes, header)
		if err != nil {
			log.Error("failed to unmarshal msg header", zap.Int("index", i), zap.Error(err))
			return err
		}

		if header.GetBase() == nil {
			log.Error("msg header base is nil", zap.Int("index", i))
			return err
		}

		if commonpb.MsgType_TimeTick == header.GetBase().GetMsgType() {
			timeTickMsg = false
		}
		cnt = i
	}

	if cnt == 0 && timeTickMsg {
		return nil
	}

	return m.DBOp(ctx, func(dbClient *grpc.ClientConn) error {
		c := pb.NewHandleReceiveMsgServiceClient(dbClient)
		//  receive_msg.HandleReceiveMsgServiceClient
		req := &pb.ReplicaMsgRequest{
			MsgType:      "replication",
			TargetDbType: m.targetDBType,
			Token:        m.token,
			Uri:          m.uri,
			Username:     m.username,
			Password:     m.password,
			ProjectId:    m.projectId,
			Database:     m.database,
			Collection:   m.collection,
			MsgBytes:     m.msgBytes,
		}

		// 다른 서버로 gRPC 메서드 호출
		otherCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		res, err := c.SendReceiveMsg(otherCtx, req)
		if err != nil {
			log.Warn("Failed to send message to other server", zap.Error(err))
			return err
		}

		if res.Status == "failed" {
			return fmt.Errorf("failed to process message: %s", res.Message)
		}

		return nil
	})
}

func (m *Server) CreateCollection(ctx context.Context, param *api.CreateCollectionParam) error {
	messageJSON, err := json.Marshal(param)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	var msg [][]byte
	msg = append(msg, messageJSON)

	m.msgBytes = msg

	return m.DBOp(ctx, func(dbClient *grpc.ClientConn) error {
		c := pb.NewHandleReceiveMsgServiceClient(dbClient)
		//  receive_msg.HandleReceiveMsgServiceClient
		req := &pb.ReplicaMsgRequest{
			MsgType:      "create_collection",
			TargetDbType: m.targetDBType,
			Token:        m.token,
			Uri:          m.uri,
			Username:     m.username,
			Password:     m.password,
			ProjectId:    m.projectId,
			Database:     m.database,
			Collection:   m.collection,
			MsgBytes:     m.msgBytes,
		}

		// 다른 서버로 gRPC 메서드 호출
		otherCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		res, err := c.SendReceiveMsg(otherCtx, req)
		if err != nil {
			log.Warn("Failed to send message to other server", zap.Error(err))
			return err
		}

		if res.Status == "failed" {
			return fmt.Errorf("failed to process message: %s", res.Message)
		}

		return nil
	})
}

func (m *Server) DropCollection(ctx context.Context, param *api.DropCollectionParam) error {
	messageJSON, err := json.Marshal(param)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	var msg [][]byte
	msg = append(msg, messageJSON)

	m.msgBytes = msg

	return m.DBOp(ctx, func(dbClient *grpc.ClientConn) error {
		c := pb.NewHandleReceiveMsgServiceClient(dbClient)
		//  receive_msg.HandleReceiveMsgServiceClient
		req := &pb.ReplicaMsgRequest{
			MsgType:      "drop_collection",
			TargetDbType: m.targetDBType,
			Token:        m.token,
			Uri:          m.uri,
			Username:     m.username,
			Password:     m.password,
			ProjectId:    m.projectId,
			Database:     m.database,
			Collection:   m.collection,
			MsgBytes:     m.msgBytes,
		}

		// 다른 서버로 gRPC 메서드 호출
		otherCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		res, err := c.SendReceiveMsg(otherCtx, req)
		if err != nil {
			log.Warn("Failed to send message to other server", zap.Error(err))
			return err
		}

		if res.Status == "failed" {
			return fmt.Errorf("failed to process message: %s", res.Message)
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

func (m *Server) Insert(ctx context.Context, param *api.InsertParam) (err error) {
	messageJSON, err := json.Marshal(param)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	var msg [][]byte
	msg = append(msg, messageJSON)

	m.msgBytes = msg

	return m.DBOp(ctx, func(dbClient *grpc.ClientConn) error {
		c := pb.NewHandleReceiveMsgServiceClient(dbClient)
		//  receive_msg.HandleReceiveMsgServiceClient
		req := &pb.ReplicaMsgRequest{
			MsgType:      "insert",
			TargetDbType: m.targetDBType,
			Token:        m.token,
			Uri:          m.uri,
			Username:     m.username,
			Password:     m.password,
			ProjectId:    m.projectId,
			Database:     m.database,
			Collection:   m.collection,
			MsgBytes:     m.msgBytes,
		}

		// 다른 서버로 gRPC 메서드 호출
		otherCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		res, err := c.SendReceiveMsg(otherCtx, req)
		if err != nil {
			log.Warn("Failed to send message to other server", zap.Error(err))
			return err
		}

		if res.Status == "failed" {
			return fmt.Errorf("failed to process message: %s", res.Message)
		}

		return nil
	})
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

func (m *Server) Delete(ctx context.Context, param *api.DeleteParam) error {
	messageJSON, err := json.Marshal(param)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	var msg [][]byte
	msg = append(msg, messageJSON)

	m.msgBytes = msg

	return m.DBOp(ctx, func(dbClient *grpc.ClientConn) error {
		c := pb.NewHandleReceiveMsgServiceClient(dbClient)
		//  receive_msg.HandleReceiveMsgServiceClient
		req := &pb.ReplicaMsgRequest{
			MsgType:      "delete",
			TargetDbType: m.targetDBType,
			Token:        m.token,
			Uri:          m.uri,
			Username:     m.username,
			Password:     m.password,
			ProjectId:    m.projectId,
			Database:     m.database,
			Collection:   m.collection,
			MsgBytes:     m.msgBytes,
		}

		// 다른 서버로 gRPC 메서드 호출
		otherCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		res, err := c.SendReceiveMsg(otherCtx, req)
		if err != nil {
			log.Warn("Failed to send message to other server", zap.Error(err))
			return err
		}

		if res.Status == "failed" {
			return fmt.Errorf("failed to process message: %s", res.Message)
		}

		return nil
	})
}

func (m *Server) CreateIndex(ctx context.Context, param *api.CreateIndexParam) error {
	messageJSON, err := json.Marshal(param)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	var msg [][]byte
	msg = append(msg, messageJSON)

	m.msgBytes = msg

	return m.DBOp(ctx, func(dbClient *grpc.ClientConn) error {
		c := pb.NewHandleReceiveMsgServiceClient(dbClient)
		//  receive_msg.HandleReceiveMsgServiceClient
		req := &pb.ReplicaMsgRequest{
			MsgType:      "create_index",
			TargetDbType: m.targetDBType,
			Token:        m.token,
			Uri:          m.uri,
			Username:     m.username,
			Password:     m.password,
			ProjectId:    m.projectId,
			Database:     m.database,
			Collection:   m.collection,
			MsgBytes:     m.msgBytes,
		}

		// 다른 서버로 gRPC 메서드 호출
		otherCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		res, err := c.SendReceiveMsg(otherCtx, req)
		if err != nil {
			log.Warn("Failed to send message to other server", zap.Error(err))
			return err
		}

		if res.Status == "failed" {
			return fmt.Errorf("failed to process message: %s", res.Message)
		}

		return nil
	})
}

func (m *DataHandler) DropIndex(ctx context.Context, param *api.DropIndexParam) error {
	// BigQuery는 인덱스를 지원하지 않습니다.
	return nil
}

func (m *Server) CreateDatabase(ctx context.Context, param *api.CreateDatabaseParam) error {
	messageJSON, err := json.Marshal(param)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	var msg [][]byte
	msg = append(msg, messageJSON)

	m.msgBytes = msg

	return m.DBOp(ctx, func(dbClient *grpc.ClientConn) error {
		c := pb.NewHandleReceiveMsgServiceClient(dbClient)
		//  receive_msg.HandleReceiveMsgServiceClient
		req := &pb.ReplicaMsgRequest{
			MsgType:      "create_database",
			TargetDbType: m.targetDBType,
			Token:        m.token,
			Uri:          m.uri,
			Username:     m.username,
			Password:     m.password,
			ProjectId:    m.projectId,
			Database:     m.database,
			Collection:   m.collection,
			MsgBytes:     m.msgBytes,
		}

		// 다른 서버로 gRPC 메서드 호출
		otherCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		res, err := c.SendReceiveMsg(otherCtx, req)
		if err != nil {
			log.Warn("Failed to send message to other server", zap.Error(err))
			return err
		}

		if res.Status == "failed" {
			return fmt.Errorf("failed to process message: %s", res.Message)
		}

		return nil
	})
}

func (m *Server) DropDatabase(ctx context.Context, param *api.DropDatabaseParam) error {
	messageJSON, err := json.Marshal(param)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	var msg [][]byte
	msg = append(msg, messageJSON)

	m.msgBytes = msg

	return m.DBOp(ctx, func(dbClient *grpc.ClientConn) error {
		c := pb.NewHandleReceiveMsgServiceClient(dbClient)
		//  receive_msg.HandleReceiveMsgServiceClient
		req := &pb.ReplicaMsgRequest{
			MsgType:      "drop_database",
			TargetDbType: m.targetDBType,
			Token:        m.token,
			Uri:          m.uri,
			Username:     m.username,
			Password:     m.password,
			ProjectId:    m.projectId,
			Database:     m.database,
			Collection:   m.collection,
			MsgBytes:     m.msgBytes,
		}

		// 다른 서버로 gRPC 메서드 호출
		otherCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		res, err := c.SendReceiveMsg(otherCtx, req)
		if err != nil {
			log.Warn("Failed to send message to other server", zap.Error(err))
			return err
		}

		if res.Status == "failed" {
			return fmt.Errorf("failed to process message: %s", res.Message)
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

		var s Server
		s.DataHandler = *m
		err = s.ReplicaMessageHandler(ctx, param)
		if err != nil {
			log.Warn("failed to replca message handle", zap.Int("index", i), zap.Error(err))
			return err
		}
	}

	return nil
}

func (m *Server) DescribeCollection(ctx context.Context, param *api.DescribeCollectionParam) error {
	messageJSON, err := json.Marshal(param)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	var msg [][]byte
	msg = append(msg, messageJSON)

	m.msgBytes = msg

	return m.DBOp(ctx, func(dbClient *grpc.ClientConn) error {
		c := pb.NewHandleReceiveMsgServiceClient(dbClient)
		//  receive_msg.HandleReceiveMsgServiceClient
		req := &pb.ReplicaMsgRequest{
			MsgType:      "describe_collection",
			TargetDbType: m.targetDBType,
			Token:        m.token,
			Uri:          m.uri,
			Username:     m.username,
			Password:     m.password,
			ProjectId:    m.projectId,
			Database:     m.database,
			Collection:   m.collection,
			MsgBytes:     m.msgBytes,
		}

		// 다른 서버로 gRPC 메서드 호출
		otherCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		res, err := c.SendReceiveMsg(otherCtx, req)
		if err != nil {
			log.Warn("Failed to send message to other server", zap.Error(err))
			return err
		}

		if res.Status == "failed" {
			return fmt.Errorf("failed to process message: %s", res.Message)
		}

		return nil
	})
}

func (m *Server) DescribeDatabase(ctx context.Context, param *api.DescribeDatabaseParam) error {
	messageJSON, err := json.Marshal(param)
	if err != nil {
		log.Warn("Error marshalling JSON:", zap.Error(err))
		return err
	}

	var msg [][]byte
	msg = append(msg, messageJSON)

	m.msgBytes = msg

	return m.DBOp(ctx, func(dbClient *grpc.ClientConn) error {
		c := pb.NewHandleReceiveMsgServiceClient(dbClient)
		//  receive_msg.HandleReceiveMsgServiceClient
		req := &pb.ReplicaMsgRequest{
			MsgType:      "describe_database",
			TargetDbType: m.targetDBType,
			Token:        m.token,
			Uri:          m.uri,
			Username:     m.username,
			Password:     m.password,
			ProjectId:    m.projectId,
			Database:     m.database,
			Collection:   m.collection,
			MsgBytes:     m.msgBytes,
		}

		// 다른 서버로 gRPC 메서드 호출
		otherCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		res, err := c.SendReceiveMsg(otherCtx, req)
		if err != nil {
			log.Warn("Failed to send message to other server", zap.Error(err))
			return err
		}

		if res.Status == "failed" {
			return fmt.Errorf("failed to process message: %s", res.Message)
		}

		return nil
	})
}
