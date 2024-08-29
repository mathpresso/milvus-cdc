/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * //
 *     http://www.apache.org/licenses/LICENSE-2.0
 * //
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package writer

import (
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/util"
)

func MilvusTokenOption(token string) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.token = token
	})
}

func MilvusURIOption(uri string) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.uri = uri
	})
}

func MilvusConnectTimeoutOption(timeout int) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		if timeout > 0 {
			object.connectTimeout = timeout
		}
	})
}

func MilvusIgnorePartitionOption(ignore bool) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.ignorePartition = ignore
	})
}

func MilvusDialConfigOption(dialConfig util.DialConfig) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.dialConfig = dialConfig
	})
}

func AgentHost(host string) config.Option[*DataHandler] {
	return config.OptionFunc[*DataHandler](func(object *DataHandler) {
		object.agentHost = host
	})
}

func AgentPort(port int) config.Option[*DataHandler] {
	return config.OptionFunc[*DataHandler](func(object *DataHandler) {
		object.agentPort = port
	})
}

func TokenOption(token string) config.Option[*DataHandler] {
	return config.OptionFunc[*DataHandler](func(object *DataHandler) {
		object.token = token
	})
}

func URIOption(uri string) config.Option[*DataHandler] {
	return config.OptionFunc[*DataHandler](func(object *DataHandler) {
		object.uri = uri
	})
}

func TargetDBTypeOption(targetDBType string) config.Option[*DataHandler] {
	return config.OptionFunc[*DataHandler](func(object *DataHandler) {
		object.targetDBType = targetDBType
	})
}

func ConnectTimeoutOption(timeout int) config.Option[*DataHandler] {
	return config.OptionFunc[*DataHandler](func(object *DataHandler) {
		if timeout > 0 {
			object.connectTimeout = timeout
		}
	})
}

func IgnorePartitionOption(ignore bool) config.Option[*DataHandler] {
	return config.OptionFunc[*DataHandler](func(object *DataHandler) {
		object.ignorePartition = ignore
	})
}

func DialConfigOption(dialConfig util.DialConfig) config.Option[*DataHandler] {
	return config.OptionFunc[*DataHandler](func(object *DataHandler) {
		object.dialConfig = dialConfig
	})
}
