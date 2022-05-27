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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/jaswdr/faker"
	"github.com/stretchr/testify/require"
)

func TestParseConfig(t *testing.T) {
	fakerInstance := faker.New()

	t.Run("Fails when Connection String is empty", func(t *testing.T) {
		_, err := ParseConfig(map[string]string{
			"nonExistentKey": "value",
		})

		require.EqualError(t, err, fmt.Sprintf("%q config value must be set", ConfigKeyConnectionString))
	})

	t.Run("Fails when Container Name is empty", func(t *testing.T) {
		_, err := ParseConfig(map[string]string{
			ConfigKeyConnectionString: fakerInstance.Internet().Query(),
			"nonExistentKey":          "value",
		})

		require.EqualError(t, err, fmt.Sprintf("%q config value must be set", ConfigKeyContainerName))
	})

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

	t.Run("Fails when Pooling Period has invalid format", func(t *testing.T) {
		_, err := ParseConfig(map[string]string{
			ConfigKeyConnectionString: fakerInstance.Internet().Query(),
			ConfigKeyContainerName:    fakerInstance.Lorem().Word(),
			ConfigKeyPollingPeriod:    "non-date format string",
			"nonExistentKey":          "value",
		})

		require.EqualError(t, err, fmt.Sprintf("%q config value should be a valid duration", ConfigKeyPollingPeriod))
	})

	for _, tt := range []struct {
		poolingPeriod string
	}{
		{
			poolingPeriod: "-1s",
		},
		{
			poolingPeriod: "0s",
		},
	} {
		t.Run(fmt.Sprintf("Fails when Pooling Period is: %s", tt.poolingPeriod), func(t *testing.T) {
			_, err := ParseConfig(map[string]string{
				ConfigKeyConnectionString: fakerInstance.Internet().Query(),
				ConfigKeyContainerName:    fakerInstance.Lorem().Word(),
				ConfigKeyPollingPeriod:    tt.poolingPeriod,
				"nonExistentKey":          "value",
			})

			require.EqualError(t, err, fmt.Sprintf("%q config value should be positive, got %s", ConfigKeyPollingPeriod, tt.poolingPeriod))
		})
	}

	t.Run("Fails when Max Results is not valid integer string", func(t *testing.T) {
		cfgRaw := map[string]string{
			ConfigKeyConnectionString: fakerInstance.Internet().Query(),
			ConfigKeyContainerName:    fakerInstance.Lorem().Word(),
			ConfigKeyMaxResults:       "non-integer format string",
			"nonExistentKey":          "value",
		}

		_, err := ParseConfig(cfgRaw)

		require.EqualError(t, err, fmt.Sprintf("failed to parse %q config value: strconv.ParseInt: parsing %q: invalid syntax", ConfigKeyMaxResults, cfgRaw[ConfigKeyMaxResults]))
	})

	for _, tt := range []struct {
		maxResults int64
	}{
		{
			maxResults: -1,
		},
		{
			maxResults: 0,
		},
	} {
		t.Run(fmt.Sprintf("Fails when Max Reuslts is less than 1: %d", tt.maxResults), func(t *testing.T) {
			cfgRaw := map[string]string{
				ConfigKeyConnectionString: fakerInstance.Internet().Query(),
				ConfigKeyContainerName:    fakerInstance.Lorem().Word(),
				ConfigKeyMaxResults:       strconv.FormatInt(tt.maxResults, 10),
				"nonExistentKey":          "value",
			}

			_, err := ParseConfig(cfgRaw)

			require.EqualError(t, err, fmt.Sprintf("failed to parse %q config value: value must be greater than 0, %s provided", ConfigKeyMaxResults, cfgRaw[ConfigKeyMaxResults]))
		})
	}

	t.Run("Fails when Max Reuslts is greater than 5000", func(t *testing.T) {
		cfgRaw := map[string]string{
			ConfigKeyConnectionString: fakerInstance.Internet().Query(),
			ConfigKeyContainerName:    fakerInstance.Lorem().Word(),
			ConfigKeyMaxResults:       "5001",
			"nonExistentKey":          "value",
		}

		_, err := ParseConfig(cfgRaw)

		require.EqualError(t, err, fmt.Sprintf("failed to parse %q config value: value must not be grater than 5000, %s provided", ConfigKeyMaxResults, cfgRaw[ConfigKeyMaxResults]))
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
