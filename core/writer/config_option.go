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

func AddressOption(address string) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.address = address
	})
}

func UserOption(username string, password string) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.username = username
		object.password = password
	})
}

func TLSOption(enable bool) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.enableTLS = enable
	})
}

func ConnectTimeoutOption(timeout int) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		if timeout > 0 {
			object.connectTimeout = timeout
		}
	})
}

func IgnorePartitionOption(ignore bool) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.ignorePartition = ignore
	})
}

func DialConfigOption(dialConfig util.DialConfig) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.dialConfig = dialConfig
	})
}

func MilvusAddressOption(address string) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.address = address
	})
}

func MilvusUserOption(username string, password string) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.username = username
		object.password = password
	})
}

func MilvusTLSOption(enable bool) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.enableTLS = enable
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

func MySQLAddressOption(address string) config.Option[*MySQLDataHandler] {
	return config.OptionFunc[*MySQLDataHandler](func(object *MySQLDataHandler) {
		object.address = address
	})
}

func MySQLUserOption(username string, password string) config.Option[*MySQLDataHandler] {
	return config.OptionFunc[*MySQLDataHandler](func(object *MySQLDataHandler) {
		object.username = username
		object.password = password
	})
}

func MySQLConnectTimeoutOption(timeout int) config.Option[*MySQLDataHandler] {
	return config.OptionFunc[*MySQLDataHandler](func(object *MySQLDataHandler) {
		if timeout > 0 {
			object.connectTimeout = timeout
		}
	})
}

func ProjectOption(projectID string) config.Option[*BigQueryDataHandler] {
	return config.OptionFunc[*BigQueryDataHandler](func(object *BigQueryDataHandler) {
		object.projectID = projectID
	})
}

func BigQueryConnectTimeoutOption(timeout int) config.Option[*BigQueryDataHandler] {
	return config.OptionFunc[*BigQueryDataHandler](func(object *BigQueryDataHandler) {
		if timeout > 0 {
			object.connectTimeout = timeout
		}
	})
}

func ESUserOption(username string, password string) config.Option[*ESDataHandler] {
	return config.OptionFunc[*ESDataHandler](func(object *ESDataHandler) {
		object.username = username
		object.password = password
	})
}

func ESAddressOption(address string) config.Option[*ESDataHandler] {
	return config.OptionFunc[*ESDataHandler](func(object *ESDataHandler) {
		object.address = address
	})
}

func ESConnectTimeoutOption(timeout int) config.Option[*ESDataHandler] {
	return config.OptionFunc[*ESDataHandler](func(object *ESDataHandler) {
		if timeout > 0 {
			object.connectTimeout = timeout
		}
	})
}
