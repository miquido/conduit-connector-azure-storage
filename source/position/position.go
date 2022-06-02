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

// NewFromRecordPosition creates a new Position by decoding sdk.Position.
func NewFromRecordPosition(recordPosition sdk.Position) (out Position, err error) {
	// Empty record position results in empty snapshot Position
	if recordPosition == nil {
		return NewDefaultSnapshotPosition(), nil
	}

	// Try to decode record position into Position
	var buffer bytes.Buffer

	buffer.Write(recordPosition)

	err = gob.NewDecoder(&buffer).Decode(&out)

	return
}

// NewDefaultSnapshotPosition creates a new Position object with Position.Type set to TypeSnapshot, empty Position.Key
// and Position.Timestamp set to zero value.
func NewDefaultSnapshotPosition() Position {
	return Position{Type: TypeSnapshot}
}

// NewSnapshotPosition creates a new Position object with Position.Type set to TypeSnapshot and other properties filled
// with given values.
func NewSnapshotPosition(key string, timestamp time.Time) Position {
	return Position{
		Key:       key,
		Timestamp: timestamp,
		Type:      TypeSnapshot,
	}
}

// NewCDCPosition creates a new Position object with Position.Type set to TypeCDC and other properties filled with
// given values.
func NewCDCPosition(key string, timestamp time.Time) Position {
	return Position{
		Key:       key,
		Timestamp: timestamp,
		Type:      TypeCDC,
	}
}

// Position represents blob item position metadata.
type Position struct {
	Key       string
	Timestamp time.Time
	Type      Type
}

// ToRecordPosition converts Position into sdk.Position.
func (p Position) ToRecordPosition() (sdk.Position, error) {
	var buffer bytes.Buffer

	if err := gob.NewEncoder(&buffer).Encode(p); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}
