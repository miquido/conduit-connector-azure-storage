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

//go:build unit

package source

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/jaswdr/faker"
	"github.com/stretchr/testify/require"
)

func TestParseConfig(t *testing.T) {
	fakerInstance := faker.New()

	for _, tt := range []struct {
		name  string
		error string
		cfg   map[string]string
	}{
		{
			name:  "Connection String is empty",
			error: fmt.Sprintf("%q config value must be set", ConfigKeyConnectionString),
			cfg: map[string]string{
				"nonExistentKey": "value",
			},
		},
		{
			name:  "Container Name is empty",
			error: fmt.Sprintf("%q config value must be set", ConfigKeyContainerName),
			cfg: map[string]string{
				ConfigKeyConnectionString: fakerInstance.Internet().Query(),
				"nonExistentKey":          "value",
			},
		},
		{
			name:  "Pooling Period has invalid format",
			error: fmt.Sprintf("%q config value should be a valid duration", ConfigKeyPollingPeriod),
			cfg: map[string]string{
				ConfigKeyConnectionString: fakerInstance.Internet().Query(),
				ConfigKeyContainerName:    fakerInstance.Lorem().Word(),
				ConfigKeyPollingPeriod:    "non-date format string",
				"nonExistentKey":          "value",
			},
		},
		{
			name:  "Pooling Period is negative",
			error: fmt.Sprintf("%q config value should be positive, got -1s", ConfigKeyPollingPeriod),
			cfg: map[string]string{
				ConfigKeyConnectionString: fakerInstance.Internet().Query(),
				ConfigKeyContainerName:    fakerInstance.Lorem().Word(),
				ConfigKeyPollingPeriod:    "-1s",
				"nonExistentKey":          "value",
			},
		},
		{
			name:  "Pooling Period is zero",
			error: fmt.Sprintf("%q config value should be positive, got 0s", ConfigKeyPollingPeriod),
			cfg: map[string]string{
				ConfigKeyConnectionString: fakerInstance.Internet().Query(),
				ConfigKeyContainerName:    fakerInstance.Lorem().Word(),
				ConfigKeyPollingPeriod:    "0s",
				"nonExistentKey":          "value",
			},
		},
		{
			name:  "Max Results is not valid integer string",
			error: fmt.Sprintf("failed to parse %q config value: strconv.ParseInt: parsing \"non-integer format string\": invalid syntax", ConfigKeyMaxResults),
			cfg: map[string]string{
				ConfigKeyConnectionString: fakerInstance.Internet().Query(),
				ConfigKeyContainerName:    fakerInstance.Lorem().Word(),
				ConfigKeyMaxResults:       "non-integer format string",
				"nonExistentKey":          "value",
			},
		},
		{
			name:  "Max Results is negative",
			error: fmt.Sprintf("failed to parse %q config value: value must be greater than 0, -1 provided", ConfigKeyMaxResults),
			cfg: map[string]string{
				ConfigKeyConnectionString: fakerInstance.Internet().Query(),
				ConfigKeyContainerName:    fakerInstance.Lorem().Word(),
				ConfigKeyMaxResults:       "-1",
				"nonExistentKey":          "value",
			},
		},
		{
			name:  "Max Results is zero",
			error: fmt.Sprintf("failed to parse %q config value: value must be greater than 0, 0 provided", ConfigKeyMaxResults),
			cfg: map[string]string{
				ConfigKeyConnectionString: fakerInstance.Internet().Query(),
				ConfigKeyContainerName:    fakerInstance.Lorem().Word(),
				ConfigKeyMaxResults:       "0",
				"nonExistentKey":          "value",
			},
		},
		{
			name:  "Max Results is greater than 5000",
			error: fmt.Sprintf("failed to parse %q config value: value must not be grater than 5000, 5001 provided", ConfigKeyMaxResults),
			cfg: map[string]string{
				ConfigKeyConnectionString: fakerInstance.Internet().Query(),
				ConfigKeyContainerName:    fakerInstance.Lorem().Word(),
				ConfigKeyMaxResults:       "5001",
				"nonExistentKey":          "value",
			},
		},
	} {
		t.Run(fmt.Sprintf("Fails when: %s", tt.name), func(t *testing.T) {
			_, err := ParseConfig(tt.cfg)

			require.EqualError(t, err, tt.error)
		})
	}

	t.Run("Returns config when all required config values were provided", func(t *testing.T) {
		cfgRaw := map[string]string{
			ConfigKeyConnectionString: fakerInstance.Internet().Query(),
			ConfigKeyContainerName:    fakerInstance.Lorem().Word(),
			"nonExistentKey":          "value",
		}

		config, err := ParseConfig(cfgRaw)

		require.NoError(t, err)
		require.Equal(t, cfgRaw[ConfigKeyConnectionString], config.ConnectionString)
		require.Equal(t, cfgRaw[ConfigKeyContainerName], config.ContainerName)
		require.Equal(t, time.Second, config.PollingPeriod)
		require.Equal(t, DefaultMaxResults, config.MaxResults)
	})

	t.Run("Returns config when all config values were provided", func(t *testing.T) {
		var (
			poolingPeriodSeconds      = fakerInstance.IntBetween(0, 59)
			poolingPeriodMilliseconds = fakerInstance.IntBetween(1, 999)
			maxResults                = fakerInstance.Int64Between(1, 5000)
		)

		cfgRaw := map[string]string{
			ConfigKeyConnectionString: fakerInstance.Internet().Query(),
			ConfigKeyContainerName:    fakerInstance.Lorem().Word(),
			ConfigKeyPollingPeriod:    fmt.Sprintf("%d.%03ds", poolingPeriodSeconds, poolingPeriodMilliseconds),
			ConfigKeyMaxResults:       strconv.FormatInt(maxResults, 10),
			"nonExistentKey":          "value",
		}

		config, err := ParseConfig(cfgRaw)

		require.NoError(t, err)
		require.Equal(t, cfgRaw[ConfigKeyConnectionString], config.ConnectionString)
		require.Equal(t, cfgRaw[ConfigKeyContainerName], config.ContainerName)
		require.Equal(t, cfgRaw[ConfigKeyPollingPeriod], fmt.Sprintf("%.3fs", float64(config.PollingPeriod.Milliseconds())/1000.0))
		require.EqualValues(t, maxResults, config.MaxResults)
	})
}
