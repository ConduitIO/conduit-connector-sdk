// Copyright © 2026 Meroxa, Inc.
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

package sdk

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// positionHashSalt is random per process, so a logged hash cannot be reversed
// by hashing guessed values and cannot be matched across processes.
var positionHashSalt = func() []byte {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("cannot read random bytes for the log hash: %v", err))
	}
	return b
}()

// positionHash returns a short, salted digest of a record position for log
// lines and errors. A position can embed record key values (a snapshot
// position holds the last primary key), so it is never logged as is. The same
// position gives the same digest within a process, so lines about one
// position can still be correlated.
func positionHash(pos []byte) string {
	mac := hmac.New(sha256.New, positionHashSalt)
	mac.Write(pos)
	return hex.EncodeToString(mac.Sum(nil)[:8])
}
