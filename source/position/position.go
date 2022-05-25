// Copyright © 2022 Meroxa, Inc. and Miquido
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

package position

import (
	"bytes"
	"encoding/gob"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	TypeSnapshot Type = iota
	TypeCDC
)

type Type int

func NewFromRecordPosition(recordPosition sdk.Position) (out Position, err error) {
	if recordPosition == nil {
		return
	}

	var buffer bytes.Buffer

	buffer.Write(recordPosition)

	err = gob.NewDecoder(&buffer).Decode(&out)

	return
}

func NewSnapshotPosition() Position {
	return Position{Type: TypeSnapshot}
}

type Position struct {
	Key       string
	Timestamp time.Time
	Type      Type
}

func (p Position) ToRecordPosition() (sdk.Position, error) {
	var buffer bytes.Buffer

	if err := gob.NewEncoder(&buffer).Encode(p); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}
