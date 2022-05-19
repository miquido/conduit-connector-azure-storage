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

package source

import (
	"context"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Source struct {
	sdk.UnimplementedSource

	config Config
}

func NewSource() sdk.Source {
	return &Source{}
}

func (s *Source) Configure(_ context.Context, cfgRaw map[string]string) (err error) {
	s.config, err = ParseConfig(cfgRaw)
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}

	return nil
}

func (s *Source) Open(ctx context.Context, _ sdk.Position) error {
	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	return sdk.Record{}, nil
}

func (s *Source) Ack(_ context.Context, _ sdk.Position) error {
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	return nil
}
