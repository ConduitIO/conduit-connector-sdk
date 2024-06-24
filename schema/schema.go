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
	"fmt"

	cschema "github.com/conduitio/conduit-commons/schema"
	pschema "github.com/conduitio/conduit-connector-protocol/conduit/schema"
)

var Service = pschema.NewInMemoryService()

func Create(ctx context.Context, typ cschema.Type, name string, bytes []byte) (cschema.Instance, error) {
	if typ != cschema.TypeAvro {
		return cschema.Instance{}, fmt.Errorf("type %v is not supported (only Avro is supported)", typ)
	}

	resp, err := Service.Create(ctx, pschema.CreateRequest{
		Subject: name,
		Type:    pschema.Type(typ),
		Bytes:   bytes,
	})
	if err != nil {
		return cschema.Instance{}, err
	}

	return resp.Instance, nil
}

func Get(ctx context.Context, name string, version int) (cschema.Instance, error) {
	resp, err := Service.Get(ctx, pschema.GetRequest{
		Subject: name,
		Version: version,
	})
	if err != nil {
		return cschema.Instance{}, err
	}

	return resp.Instance, nil
}
