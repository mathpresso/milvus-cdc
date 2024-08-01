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
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/util/resource"

	"github.com/zilliztech/milvus-cdc/core/log"
)

const (
	BigQueryClientResourceTyp = "bigquery_client"
	BigQueryClientExpireTime  = 30 * time.Second
	DefaultBigQueryDbName     = "default"
)

func (m *ClientResourceManager) newBigQueryClient(ctx context.Context, projectId string) resource.NewResourceFunc {
	return func() (resource.Resource, error) {

		c, err := bigquery.NewClient(ctx,
			projectId,
		)
		if err != nil {
			log.Warn("fail to new the bigquery client", zap.String("project_id", projectId), zap.Error(err))
			return nil, err
		}

		res := resource.NewSimpleResource(c, BigQueryClientResourceTyp, projectId, BigQueryClientExpireTime, func() {
			_ = c.Close()
		})

		return res, nil
	}
}

func (m *ClientResourceManager) GetBigQueryClient(ctx context.Context, projectId, database string) (*bigquery.Client, error) {
	if database == "" {
		return nil, errors.New("empty database name")
	}

	ctxLog := log.Ctx(ctx).With(zap.String("database", database), zap.String("project_id", projectId))
	res, err := m.manager.Get(BigQueryClientResourceTyp,
		getBigQueryClientResourceName(projectId, database),
		m.newBigQueryClient(ctx, projectId))
	if err != nil {
		ctxLog.Error("fail to get bigquery client", zap.Error(err))
		return nil, err
	}
	if obj, ok := res.Get().(*bigquery.Client); ok && obj != nil {
		return obj, nil
	}
	ctxLog.Warn("invalid resource object", zap.Any("obj", reflect.TypeOf(res.Get())))
	return nil, errors.New("invalid resource object")
}

func getBigQueryClientResourceName(projectId, database string) string {
	return fmt.Sprintf("%s:%s", projectId, database)
}
