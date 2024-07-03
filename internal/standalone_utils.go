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

package internal

import "google.golang.org/grpc"

var StandaloneConnectorUtilities []ConnectorUtilityService

type ConnectorUtilityService interface {
	Init(conn *grpc.ClientConn) error
}

func InitStandaloneConnectorUtilities(target string) error {
	if len(StandaloneConnectorUtilities) == 0 {
		return nil
	}

	conn, err := grpc.NewClient(target)
	if err != nil {
		return err
	}

	for _, utility := range StandaloneConnectorUtilities {
		if err := utility.Init(conn); err != nil {
			return err
		}
	}
	return nil
}
