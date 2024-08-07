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

package util

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"reflect"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/util/resource"

	"github.com/zilliztech/milvus-cdc/core/log"
)

const (
	MySQLClientResourceTyp = "mysql_client"
	DefaultMySQLDbName     = "information_schema"
	ConnectionTimeout      = 30
	MySQLClientExpireTime  = ConnectionTimeout * time.Second
)

func (m *ClientResourceManager) newMySQLClient(address, user, passwd, database string, enableTLS bool, connectionTimeout int) resource.NewResourceFunc {
	return func() (resource.Resource, error) {
		if connectionTimeout == 0 {
			connectionTimeout = ConnectionTimeout
		}
		cfg := mysql.Config{
			Addr:                 address,
			User:                 user,
			Passwd:               passwd,
			Net:                  "tcp",
			AllowNativePasswords: true,
			Timeout:              time.Duration(connectionTimeout) * time.Second,
		}

		db, err := sql.Open("mysql", cfg.FormatDSN())
		if err != nil {
			log.Warn("fail to new the mysql client", zap.String("database", database), zap.String("address", address), zap.Error(err))
			return nil, err
		}

		res := resource.NewSimpleResource(db, MySQLClientResourceTyp, fmt.Sprintf("%s:%s", address, database), MySQLClientExpireTime, func() {
			_ = db.Close()
		})

		return res, nil
	}
}

func (m *ClientResourceManager) GetMySQLClient(ctx context.Context, address, user, passwd, database string, enableTLS bool, connectionTimeout int) (*sql.DB, error) {
	if database == "" {
		database = DefaultMySQLDbName
	}

	ctxLog := log.Ctx(ctx).With(zap.String("database", database), zap.String("address", address))
	res, err := m.manager.Get(MySQLClientResourceTyp,
		getMySQLClientResourceName(address, database),
		m.newMySQLClient(address, user, passwd, database, enableTLS, connectionTimeout))
	if err != nil {
		ctxLog.Error("fail to get mysql client", zap.Error(err))
		return nil, err
	}
	if obj, ok := res.Get().(*sql.DB); ok && obj != nil {
		return obj, nil
	}
	ctxLog.Warn("invalid resource object", zap.Any("obj", reflect.TypeOf(res.Get())))
	return nil, errors.New("invalid resource object")
}

func getMySQLClientResourceName(address, database string) string {
	return fmt.Sprintf("%s:%s", address, database)
}
