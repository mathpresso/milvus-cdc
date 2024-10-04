// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.2
// 	protoc        v5.27.3
// source: receive_msg.proto

package receive_msg

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type RetryOption struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	MaxRetries  int32 `protobuf:"varint,1,opt,name=max_retries,json=maxRetries,proto3" json:"max_retries,omitempty"`
	Initbackoff int32 `protobuf:"varint,2,opt,name=initbackoff,proto3" json:"initbackoff,omitempty"`
	Maxbackoff  int32 `protobuf:"varint,3,opt,name=maxbackoff,proto3" json:"maxbackoff,omitempty"`
}

func (x *RetryOption) Reset() {
	*x = RetryOption{}
	if protoimpl.UnsafeEnabled {
		mi := &file_receive_msg_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RetryOption) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RetryOption) ProtoMessage() {}

func (x *RetryOption) ProtoReflect() protoreflect.Message {
	mi := &file_receive_msg_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RetryOption.ProtoReflect.Descriptor instead.
func (*RetryOption) Descriptor() ([]byte, []int) {
	return file_receive_msg_proto_rawDescGZIP(), []int{0}
}

func (x *RetryOption) GetMaxRetries() int32 {
	if x != nil {
		return x.MaxRetries
	}
	return 0
}

func (x *RetryOption) GetInitbackoff() int32 {
	if x != nil {
		return x.Initbackoff
	}
	return 0
}

func (x *RetryOption) GetMaxbackoff() int32 {
	if x != nil {
		return x.Maxbackoff
	}
	return 0
}

type DialConfig struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Servername    string `protobuf:"bytes,1,opt,name=servername,proto3" json:"servername,omitempty"`
	Serverpempath string `protobuf:"bytes,2,opt,name=serverpempath,proto3" json:"serverpempath,omitempty"`
	Capempath     string `protobuf:"bytes,3,opt,name=capempath,proto3" json:"capempath,omitempty"`
	Clientpempath string `protobuf:"bytes,4,opt,name=clientpempath,proto3" json:"clientpempath,omitempty"`
	Clientkeypath string `protobuf:"bytes,5,opt,name=clientkeypath,proto3" json:"clientkeypath,omitempty"`
}

func (x *DialConfig) Reset() {
	*x = DialConfig{}
	if protoimpl.UnsafeEnabled {
		mi := &file_receive_msg_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DialConfig) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DialConfig) ProtoMessage() {}

func (x *DialConfig) ProtoReflect() protoreflect.Message {
	mi := &file_receive_msg_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DialConfig.ProtoReflect.Descriptor instead.
func (*DialConfig) Descriptor() ([]byte, []int) {
	return file_receive_msg_proto_rawDescGZIP(), []int{1}
}

func (x *DialConfig) GetServername() string {
	if x != nil {
		return x.Servername
	}
	return ""
}

func (x *DialConfig) GetServerpempath() string {
	if x != nil {
		return x.Serverpempath
	}
	return ""
}

func (x *DialConfig) GetCapempath() string {
	if x != nil {
		return x.Capempath
	}
	return ""
}

func (x *DialConfig) GetClientpempath() string {
	if x != nil {
		return x.Clientpempath
	}
	return ""
}

func (x *DialConfig) GetClientkeypath() string {
	if x != nil {
		return x.Clientkeypath
	}
	return ""
}

type ReplicaMsgRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AgentHost       string         `protobuf:"bytes,1,opt,name=agent_host,json=agentHost,proto3" json:"agent_host,omitempty"`
	AgentPort       string         `protobuf:"bytes,2,opt,name=agent_port,json=agentPort,proto3" json:"agent_port,omitempty"`
	Token           string         `protobuf:"bytes,3,opt,name=token,proto3" json:"token,omitempty"`
	Uri             string         `protobuf:"bytes,4,opt,name=uri,proto3" json:"uri,omitempty"`
	Address         string         `protobuf:"bytes,5,opt,name=address,proto3" json:"address,omitempty"`
	Database        string         `protobuf:"bytes,6,opt,name=database,proto3" json:"database,omitempty"`
	Collection      string         `protobuf:"bytes,7,opt,name=collection,proto3" json:"collection,omitempty"`
	Pkcolumn        string         `protobuf:"bytes,8,opt,name=pkcolumn,proto3" json:"pkcolumn,omitempty"`
	Username        string         `protobuf:"bytes,9,opt,name=username,proto3" json:"username,omitempty"`
	Password        string         `protobuf:"bytes,10,opt,name=password,proto3" json:"password,omitempty"`
	EnableTls       bool           `protobuf:"varint,11,opt,name=enable_tls,json=enableTls,proto3" json:"enable_tls,omitempty"`
	IgnorePartition bool           `protobuf:"varint,12,opt,name=ignore_partition,json=ignorePartition,proto3" json:"ignore_partition,omitempty"`
	ConnectTimeout  int32          `protobuf:"varint,13,opt,name=connect_timeout,json=connectTimeout,proto3" json:"connect_timeout,omitempty"`
	ProjectId       string         `protobuf:"bytes,14,opt,name=project_id,json=projectId,proto3" json:"project_id,omitempty"`
	MsgBytes        [][]byte       `protobuf:"bytes,15,rep,name=msg_bytes,json=msgBytes,proto3" json:"msg_bytes,omitempty"`
	RetryOptions    []*RetryOption `protobuf:"bytes,16,rep,name=retry_options,json=retryOptions,proto3" json:"retry_options,omitempty"`
	DialConfig      *DialConfig    `protobuf:"bytes,17,opt,name=dial_config,json=dialConfig,proto3" json:"dial_config,omitempty"`
	TargetDbType    string         `protobuf:"bytes,18,opt,name=target_db_type,json=targetDbType,proto3" json:"target_db_type,omitempty"`
	MsgType         string         `protobuf:"bytes,19,opt,name=msg_type,json=msgType,proto3" json:"msg_type,omitempty"`
}

func (x *ReplicaMsgRequest) Reset() {
	*x = ReplicaMsgRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_receive_msg_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReplicaMsgRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReplicaMsgRequest) ProtoMessage() {}

func (x *ReplicaMsgRequest) ProtoReflect() protoreflect.Message {
	mi := &file_receive_msg_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReplicaMsgRequest.ProtoReflect.Descriptor instead.
func (*ReplicaMsgRequest) Descriptor() ([]byte, []int) {
	return file_receive_msg_proto_rawDescGZIP(), []int{2}
}

func (x *ReplicaMsgRequest) GetAgentHost() string {
	if x != nil {
		return x.AgentHost
	}
	return ""
}

func (x *ReplicaMsgRequest) GetAgentPort() string {
	if x != nil {
		return x.AgentPort
	}
	return ""
}

func (x *ReplicaMsgRequest) GetToken() string {
	if x != nil {
		return x.Token
	}
	return ""
}

func (x *ReplicaMsgRequest) GetUri() string {
	if x != nil {
		return x.Uri
	}
	return ""
}

func (x *ReplicaMsgRequest) GetAddress() string {
	if x != nil {
		return x.Address
	}
	return ""
}

func (x *ReplicaMsgRequest) GetDatabase() string {
	if x != nil {
		return x.Database
	}
	return ""
}

func (x *ReplicaMsgRequest) GetCollection() string {
	if x != nil {
		return x.Collection
	}
	return ""
}

func (x *ReplicaMsgRequest) GetPkcolumn() string {
	if x != nil {
		return x.Pkcolumn
	}
	return ""
}

func (x *ReplicaMsgRequest) GetUsername() string {
	if x != nil {
		return x.Username
	}
	return ""
}

func (x *ReplicaMsgRequest) GetPassword() string {
	if x != nil {
		return x.Password
	}
	return ""
}

func (x *ReplicaMsgRequest) GetEnableTls() bool {
	if x != nil {
		return x.EnableTls
	}
	return false
}

func (x *ReplicaMsgRequest) GetIgnorePartition() bool {
	if x != nil {
		return x.IgnorePartition
	}
	return false
}

func (x *ReplicaMsgRequest) GetConnectTimeout() int32 {
	if x != nil {
		return x.ConnectTimeout
	}
	return 0
}

func (x *ReplicaMsgRequest) GetProjectId() string {
	if x != nil {
		return x.ProjectId
	}
	return ""
}

func (x *ReplicaMsgRequest) GetMsgBytes() [][]byte {
	if x != nil {
		return x.MsgBytes
	}
	return nil
}

func (x *ReplicaMsgRequest) GetRetryOptions() []*RetryOption {
	if x != nil {
		return x.RetryOptions
	}
	return nil
}

func (x *ReplicaMsgRequest) GetDialConfig() *DialConfig {
	if x != nil {
		return x.DialConfig
	}
	return nil
}

func (x *ReplicaMsgRequest) GetTargetDbType() string {
	if x != nil {
		return x.TargetDbType
	}
	return ""
}

func (x *ReplicaMsgRequest) GetMsgType() string {
	if x != nil {
		return x.MsgType
	}
	return ""
}

type ReplicaMsgResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Status  string `protobuf:"bytes,1,opt,name=status,proto3" json:"status,omitempty"`
	Message string `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *ReplicaMsgResponse) Reset() {
	*x = ReplicaMsgResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_receive_msg_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReplicaMsgResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReplicaMsgResponse) ProtoMessage() {}

func (x *ReplicaMsgResponse) ProtoReflect() protoreflect.Message {
	mi := &file_receive_msg_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReplicaMsgResponse.ProtoReflect.Descriptor instead.
func (*ReplicaMsgResponse) Descriptor() ([]byte, []int) {
	return file_receive_msg_proto_rawDescGZIP(), []int{3}
}

func (x *ReplicaMsgResponse) GetStatus() string {
	if x != nil {
		return x.Status
	}
	return ""
}

func (x *ReplicaMsgResponse) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

var File_receive_msg_proto protoreflect.FileDescriptor

var file_receive_msg_proto_rawDesc = []byte{
	0x0a, 0x11, 0x72, 0x65, 0x63, 0x65, 0x69, 0x76, 0x65, 0x5f, 0x6d, 0x73, 0x67, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x0b, 0x72, 0x65, 0x63, 0x65, 0x69, 0x76, 0x65, 0x5f, 0x6d, 0x73, 0x67,
	0x22, 0x70, 0x0a, 0x0b, 0x52, 0x65, 0x74, 0x72, 0x79, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12,
	0x1f, 0x0a, 0x0b, 0x6d, 0x61, 0x78, 0x5f, 0x72, 0x65, 0x74, 0x72, 0x69, 0x65, 0x73, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x05, 0x52, 0x0a, 0x6d, 0x61, 0x78, 0x52, 0x65, 0x74, 0x72, 0x69, 0x65, 0x73,
	0x12, 0x20, 0x0a, 0x0b, 0x69, 0x6e, 0x69, 0x74, 0x62, 0x61, 0x63, 0x6b, 0x6f, 0x66, 0x66, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0b, 0x69, 0x6e, 0x69, 0x74, 0x62, 0x61, 0x63, 0x6b, 0x6f,
	0x66, 0x66, 0x12, 0x1e, 0x0a, 0x0a, 0x6d, 0x61, 0x78, 0x62, 0x61, 0x63, 0x6b, 0x6f, 0x66, 0x66,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0a, 0x6d, 0x61, 0x78, 0x62, 0x61, 0x63, 0x6b, 0x6f,
	0x66, 0x66, 0x22, 0xbc, 0x01, 0x0a, 0x0a, 0x44, 0x69, 0x61, 0x6c, 0x43, 0x6f, 0x6e, 0x66, 0x69,
	0x67, 0x12, 0x1e, 0x0a, 0x0a, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x6e, 0x61, 0x6d, 0x65, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x6e, 0x61, 0x6d,
	0x65, 0x12, 0x24, 0x0a, 0x0d, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x70, 0x65, 0x6d, 0x70, 0x61,
	0x74, 0x68, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x70, 0x65, 0x6d, 0x70, 0x61, 0x74, 0x68, 0x12, 0x1c, 0x0a, 0x09, 0x63, 0x61, 0x70, 0x65, 0x6d,
	0x70, 0x61, 0x74, 0x68, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x63, 0x61, 0x70, 0x65,
	0x6d, 0x70, 0x61, 0x74, 0x68, 0x12, 0x24, 0x0a, 0x0d, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x70,
	0x65, 0x6d, 0x70, 0x61, 0x74, 0x68, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x63, 0x6c,
	0x69, 0x65, 0x6e, 0x74, 0x70, 0x65, 0x6d, 0x70, 0x61, 0x74, 0x68, 0x12, 0x24, 0x0a, 0x0d, 0x63,
	0x6c, 0x69, 0x65, 0x6e, 0x74, 0x6b, 0x65, 0x79, 0x70, 0x61, 0x74, 0x68, 0x18, 0x05, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0d, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x6b, 0x65, 0x79, 0x70, 0x61, 0x74,
	0x68, 0x22, 0x8c, 0x05, 0x0a, 0x11, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x4d, 0x73, 0x67,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1d, 0x0a, 0x0a, 0x61, 0x67, 0x65, 0x6e, 0x74,
	0x5f, 0x68, 0x6f, 0x73, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x61, 0x67, 0x65,
	0x6e, 0x74, 0x48, 0x6f, 0x73, 0x74, 0x12, 0x1d, 0x0a, 0x0a, 0x61, 0x67, 0x65, 0x6e, 0x74, 0x5f,
	0x70, 0x6f, 0x72, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x61, 0x67, 0x65, 0x6e,
	0x74, 0x50, 0x6f, 0x72, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x12, 0x10, 0x0a, 0x03, 0x75,
	0x72, 0x69, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x75, 0x72, 0x69, 0x12, 0x18, 0x0a,
	0x07, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07,
	0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x12, 0x1a, 0x0a, 0x08, 0x64, 0x61, 0x74, 0x61, 0x62,
	0x61, 0x73, 0x65, 0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x64, 0x61, 0x74, 0x61, 0x62,
	0x61, 0x73, 0x65, 0x12, 0x1e, 0x0a, 0x0a, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f,
	0x6e, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74,
	0x69, 0x6f, 0x6e, 0x12, 0x1a, 0x0a, 0x08, 0x70, 0x6b, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x18,
	0x08, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x70, 0x6b, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x12,
	0x1a, 0x0a, 0x08, 0x75, 0x73, 0x65, 0x72, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x09, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x08, 0x75, 0x73, 0x65, 0x72, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x70,
	0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x70,
	0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x12, 0x1d, 0x0a, 0x0a, 0x65, 0x6e, 0x61, 0x62, 0x6c,
	0x65, 0x5f, 0x74, 0x6c, 0x73, 0x18, 0x0b, 0x20, 0x01, 0x28, 0x08, 0x52, 0x09, 0x65, 0x6e, 0x61,
	0x62, 0x6c, 0x65, 0x54, 0x6c, 0x73, 0x12, 0x29, 0x0a, 0x10, 0x69, 0x67, 0x6e, 0x6f, 0x72, 0x65,
	0x5f, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x0c, 0x20, 0x01, 0x28, 0x08,
	0x52, 0x0f, 0x69, 0x67, 0x6e, 0x6f, 0x72, 0x65, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f,
	0x6e, 0x12, 0x27, 0x0a, 0x0f, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x5f, 0x74, 0x69, 0x6d,
	0x65, 0x6f, 0x75, 0x74, 0x18, 0x0d, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0e, 0x63, 0x6f, 0x6e, 0x6e,
	0x65, 0x63, 0x74, 0x54, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x12, 0x1d, 0x0a, 0x0a, 0x70, 0x72,
	0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x0e, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09,
	0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x49, 0x64, 0x12, 0x1b, 0x0a, 0x09, 0x6d, 0x73, 0x67,
	0x5f, 0x62, 0x79, 0x74, 0x65, 0x73, 0x18, 0x0f, 0x20, 0x03, 0x28, 0x0c, 0x52, 0x08, 0x6d, 0x73,
	0x67, 0x42, 0x79, 0x74, 0x65, 0x73, 0x12, 0x3d, 0x0a, 0x0d, 0x72, 0x65, 0x74, 0x72, 0x79, 0x5f,
	0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x10, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x18, 0x2e,
	0x72, 0x65, 0x63, 0x65, 0x69, 0x76, 0x65, 0x5f, 0x6d, 0x73, 0x67, 0x2e, 0x52, 0x65, 0x74, 0x72,
	0x79, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x0c, 0x72, 0x65, 0x74, 0x72, 0x79, 0x4f, 0x70,
	0x74, 0x69, 0x6f, 0x6e, 0x73, 0x12, 0x38, 0x0a, 0x0b, 0x64, 0x69, 0x61, 0x6c, 0x5f, 0x63, 0x6f,
	0x6e, 0x66, 0x69, 0x67, 0x18, 0x11, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x72, 0x65, 0x63,
	0x65, 0x69, 0x76, 0x65, 0x5f, 0x6d, 0x73, 0x67, 0x2e, 0x44, 0x69, 0x61, 0x6c, 0x43, 0x6f, 0x6e,
	0x66, 0x69, 0x67, 0x52, 0x0a, 0x64, 0x69, 0x61, 0x6c, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x12,
	0x24, 0x0a, 0x0e, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0x5f, 0x64, 0x62, 0x5f, 0x74, 0x79, 0x70,
	0x65, 0x18, 0x12, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0x44,
	0x62, 0x54, 0x79, 0x70, 0x65, 0x12, 0x19, 0x0a, 0x08, 0x6d, 0x73, 0x67, 0x5f, 0x74, 0x79, 0x70,
	0x65, 0x18, 0x13, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6d, 0x73, 0x67, 0x54, 0x79, 0x70, 0x65,
	0x22, 0x46, 0x0a, 0x12, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x4d, 0x73, 0x67, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x18,
	0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x32, 0x6c, 0x0a, 0x17, 0x48, 0x61, 0x6e, 0x64,
	0x6c, 0x65, 0x52, 0x65, 0x63, 0x65, 0x69, 0x76, 0x65, 0x4d, 0x73, 0x67, 0x53, 0x65, 0x72, 0x76,
	0x69, 0x63, 0x65, 0x12, 0x51, 0x0a, 0x0e, 0x53, 0x65, 0x6e, 0x64, 0x52, 0x65, 0x63, 0x65, 0x69,
	0x76, 0x65, 0x4d, 0x73, 0x67, 0x12, 0x1e, 0x2e, 0x72, 0x65, 0x63, 0x65, 0x69, 0x76, 0x65, 0x5f,
	0x6d, 0x73, 0x67, 0x2e, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x4d, 0x73, 0x67, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1f, 0x2e, 0x72, 0x65, 0x63, 0x65, 0x69, 0x76, 0x65, 0x5f,
	0x6d, 0x73, 0x67, 0x2e, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x4d, 0x73, 0x67, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x42, 0x12, 0x5a, 0x10, 0x67, 0x72, 0x70, 0x63, 0x2f, 0x72,
	0x65, 0x63, 0x65, 0x69, 0x76, 0x65, 0x5f, 0x6d, 0x73, 0x67, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var (
	file_receive_msg_proto_rawDescOnce sync.Once
	file_receive_msg_proto_rawDescData = file_receive_msg_proto_rawDesc
)

func file_receive_msg_proto_rawDescGZIP() []byte {
	file_receive_msg_proto_rawDescOnce.Do(func() {
		file_receive_msg_proto_rawDescData = protoimpl.X.CompressGZIP(file_receive_msg_proto_rawDescData)
	})
	return file_receive_msg_proto_rawDescData
}

var file_receive_msg_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_receive_msg_proto_goTypes = []any{
	(*RetryOption)(nil),        // 0: receive_msg.RetryOption
	(*DialConfig)(nil),         // 1: receive_msg.DialConfig
	(*ReplicaMsgRequest)(nil),  // 2: receive_msg.ReplicaMsgRequest
	(*ReplicaMsgResponse)(nil), // 3: receive_msg.ReplicaMsgResponse
}
var file_receive_msg_proto_depIdxs = []int32{
	0, // 0: receive_msg.ReplicaMsgRequest.retry_options:type_name -> receive_msg.RetryOption
	1, // 1: receive_msg.ReplicaMsgRequest.dial_config:type_name -> receive_msg.DialConfig
	2, // 2: receive_msg.HandleReceiveMsgService.SendReceiveMsg:input_type -> receive_msg.ReplicaMsgRequest
	3, // 3: receive_msg.HandleReceiveMsgService.SendReceiveMsg:output_type -> receive_msg.ReplicaMsgResponse
	3, // [3:4] is the sub-list for method output_type
	2, // [2:3] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_receive_msg_proto_init() }
func file_receive_msg_proto_init() {
	if File_receive_msg_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_receive_msg_proto_msgTypes[0].Exporter = func(v any, i int) any {
			switch v := v.(*RetryOption); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_receive_msg_proto_msgTypes[1].Exporter = func(v any, i int) any {
			switch v := v.(*DialConfig); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_receive_msg_proto_msgTypes[2].Exporter = func(v any, i int) any {
			switch v := v.(*ReplicaMsgRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_receive_msg_proto_msgTypes[3].Exporter = func(v any, i int) any {
			switch v := v.(*ReplicaMsgResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_receive_msg_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_receive_msg_proto_goTypes,
		DependencyIndexes: file_receive_msg_proto_depIdxs,
		MessageInfos:      file_receive_msg_proto_msgTypes,
	}.Build()
	File_receive_msg_proto = out.File
	file_receive_msg_proto_rawDesc = nil
	file_receive_msg_proto_goTypes = nil
	file_receive_msg_proto_depIdxs = nil
}
