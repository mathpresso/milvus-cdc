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
 * custom by qanda
 */

package util

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/util/resource"

	"github.com/zilliztech/milvus-cdc/core/log"
)

const (
	DBClientResourceTyp = "db_client"
	DBClientExpireTime  = 30 * time.Second
)

var (
	dbClientManager     *DBClientResourceManager
	dbClientManagerOnce sync.Once
)

type DBClientResourceManager struct {
	manager resource.Manager
}

func GetDBClientManager() *DBClientResourceManager {
	dbClientManagerOnce.Do(func() {
		manager := resource.NewManager(0, 0, nil)
		dbClientManager = &DBClientResourceManager{
			manager: manager,
		}
	})
	return dbClientManager
}

func (m *DBClientResourceManager) newDBClient(cdcAgentHost, cdcAgentPort, address, database, collection string, dialConfig DialConfig) resource.NewResourceFunc {
	return func() (resource.Resource, error) {
		conn, err := net.Dial("tcp", cdcAgentHost+":"+cdcAgentPort)
		if err != nil {
			log.Warn("Error connecting:", zap.Error(err))
			return nil, err
		}
		/*
			c, err := client.NewClient(ctx, client.Config{
				Address:       address,
				APIKey:        apiKey,
				EnableTLSAuth: enableTLS,
				DBName:        database,
			})
			if err != nil {
				log.Warn("fail to new the db client", zap.String("database", database), zap.String("address", address), zap.Error(err))
				return nil, err
			}
		*/
		res := resource.NewSimpleResource(conn, DBClientResourceTyp, fmt.Sprintf("%s:%s:%s", address, database, collection), DBClientExpireTime, func() {
			_ = conn.Close()
		})

		return res, nil
	}
}

func (m *DBClientResourceManager) GetDBClient(ctx context.Context, cdcAgentHost, cdcAgentPort, address, database, collection string, dialConfig DialConfig, connectionTimeout int) (net.Conn, error) {
	if database == "" {
		database = DefaultDbName
	}
	ctxLog := log.Ctx(ctx).With(zap.String("database", database), zap.String("address", address))
	res, err := m.manager.Get(DBClientResourceTyp,
		getDBClientResourceName(address, database, collection),
		m.newDBClient(cdcAgentHost, cdcAgentPort, address, database, collection, dialConfig))
	if err != nil {
		ctxLog.Error("fail to get db client", zap.Error(err))
		return nil, err
	}
	if obj, ok := res.Get().(net.Conn); ok && obj != nil {
		return obj, nil
	}
	ctxLog.Warn("invalid resource object", zap.Any("obj", reflect.TypeOf(res.Get())))
	return nil, errors.New("invalid resource object")
}

func (m *DBClientResourceManager) DeleteDBClient(address, database, collection string) {
	_ = m.manager.Delete(DBClientResourceTyp, getDBClientResourceName(address, database, collection))
}

func getDBClientResourceName(address, database, collection string) string {
	return fmt.Sprintf("%s:%s:%s", address, database, collection)
}