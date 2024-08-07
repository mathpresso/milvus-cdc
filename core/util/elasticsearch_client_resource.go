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
	"crypto/tls"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/milvus-io/milvus/pkg/util/resource"
	"github.com/zilliztech/milvus-cdc/core/log"
	"go.uber.org/zap"
	"net/http"
	"time"
)

const (
	ESClientResourceTyp = "es_client"
	ESClientExpireTime  = 30 * time.Second
	DefaultESIndexName  = "default-index"
)

type resourceManager struct{}

func (rm *resourceManager) Get(resourceType, resourceName string, createFunc func() (resource.Resource, error)) (resource.Resource, error) {
	// Implement resource retrieval logic
	return nil, nil
}

// newMySQLClient(address, user, passwd, database string, enableTLS bool, connectionTimeout int) resource.NewResourceFunc
func (m *ClientResourceManager) newESClient(ctx context.Context, address, user, password, index string, enableTLS bool, connectionTimeout int) func() (resource.Resource, error) {
	return func() (resource.Resource, error) {
		cfg := elasticsearch.Config{
			Addresses: []string{
				address, // "http://localhost:9200"
			},
			Username: user,
			Password: password,
			//			CertificateFingerprint : "SHA256",
			//          ServiceToken: "AAA",
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // 필요시 TLS 설정
				IdleConnTimeout: time.Duration(connectionTimeout) * time.Second,
			},
		}

		es, err := elasticsearch.NewClient(cfg)
		if err != nil {
			log.Warn("fail to new the mysql client", zap.String("user", user), zap.String("address", address), zap.Error(err))

			return nil, err
		}

		res := resource.NewSimpleResource(es, "ElasticsearchClientResourceType", fmt.Sprintf("%s:%s", address, index), time.Hour, func() {
			// No explicit close method, just let GC handle it
		})

		return res, nil
	}
}

func (m *ClientResourceManager) GetESClient(ctx context.Context, address, user, passwd, index string, enableTLS bool, connectionTimeout int) (*elasticsearch.Client, error) {
	if index == "" {
		index = DefaultESIndexName
	}

	ctxLog := log.Ctx(ctx).With(zap.String("index", index), zap.String("address", address))

	res, err := m.manager.Get("ElasticsearchClientResourceType",
		getESClientResourceName(address, index),
		m.newESClient(ctx, address, user, passwd, index, enableTLS, connectionTimeout))
	if err != nil {
		ctxLog.Error("fail to get Elasticsearch client", zap.Error(err))

		return nil, err
	}
	if obj, ok := res.Get().(*elasticsearch.Client); ok && obj != nil {
		return obj, nil
	}

	ctxLog.Error("invalid resource object", zap.Any("obj", res.Get()))

	return nil, fmt.Errorf("invalid resource object")
}

func getESClientResourceName(address, index string) string {
	return fmt.Sprintf("%s:%s", address, index)
}
