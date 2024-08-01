// Copyright © 2024 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schema

import (
	"context"
	"time"

	"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-protocol/pconnutils"
	"github.com/twmb/go-cache/cache"
)

func newCachedSchemaService(service pconnutils.SchemaService) *cachedSchemaService {
	return &cachedSchemaService{
		SchemaService: service,

		getSchemaCache: cache.New[pconnutils.GetSchemaRequest, pconnutils.GetSchemaResponse](
			cache.MaxAge(15 * time.Minute), // expire entries after 15 minutes
		),
		createSchemaCache: cache.New[comparableCreateSchemaRequest, pconnutils.CreateSchemaResponse](
			cache.MaxAge(15 * time.Minute), // expire entries after 15 minutes
		),
	}
}

type cachedSchemaService struct {
	pconnutils.SchemaService

	getSchemaCache    *cache.Cache[pconnutils.GetSchemaRequest, pconnutils.GetSchemaResponse]
	createSchemaCache *cache.Cache[comparableCreateSchemaRequest, pconnutils.CreateSchemaResponse]
}

type comparableCreateSchemaRequest struct {
	Subject string
	Type    schema.Type
	Bytes   string
}

func (c *cachedSchemaService) GetSchema(ctx context.Context, request pconnutils.GetSchemaRequest) (pconnutils.GetSchemaResponse, error) {
	resp, err, _ := c.getSchemaCache.Get(request, func() (pconnutils.GetSchemaResponse, error) {
		return c.SchemaService.GetSchema(ctx, request)
	})
	return resp, err
}

func (c *cachedSchemaService) CreateSchema(ctx context.Context, request pconnutils.CreateSchemaRequest) (pconnutils.CreateSchemaResponse, error) {
	creq := comparableCreateSchemaRequest{
		Subject: request.Subject,
		Type:    request.Type,
		Bytes:   string(request.Bytes),
	}
	resp, err, _ := c.createSchemaCache.Get(creq, func() (pconnutils.CreateSchemaResponse, error) {
		return c.SchemaService.CreateSchema(ctx, request)
	})
	return resp, err
}
