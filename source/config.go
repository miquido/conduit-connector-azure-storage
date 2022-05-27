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
	"time"
)

const (
	ConfigKeyConnectionString = "connectionString"
	ConfigKeyContainerName    = "containerName"

	ConfigKeyPollingPeriod = "pollingPeriod"
	DefaultPollingPeriod   = "1s"

	ConfigKeyMaxResults       = "maxResults"
	DefaultMaxResults   int32 = 5000
)

type Config struct {
	ConnectionString string
	ContainerName    string
	PollingPeriod    time.Duration
	MaxResults       int32
}

func ParseConfig(cfgRaw map[string]string) (_ Config, err error) {
	cfg := Config{
		ConnectionString: cfgRaw[ConfigKeyConnectionString],
		ContainerName:    cfgRaw[ConfigKeyContainerName],
	}

	if cfg.ConnectionString == "" {
		return Config{}, requiredConfigErr(ConfigKeyConnectionString)
	}

	if cfg.ContainerName == "" {
		return Config{}, requiredConfigErr(ConfigKeyContainerName)
	}

	if cfg.PollingPeriod, err = parsePollingPeriod(cfgRaw); err != nil {
		return Config{}, err
	}

	if cfg.MaxResults, err = parseMaxResults(cfgRaw); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func requiredConfigErr(name string) error {
	return fmt.Errorf("%q config value must be set", name)
}

func parsePollingPeriod(cfgRaw map[string]string) (time.Duration, error) {
	pollingPeriodString, exists := cfgRaw[ConfigKeyPollingPeriod]
	if !exists || pollingPeriodString == "" {
		pollingPeriodString = DefaultPollingPeriod
	}

	pollingPeriod, err := time.ParseDuration(pollingPeriodString)
	if err != nil {
		return 0, fmt.Errorf(
			"%q config value should be a valid duration",
			ConfigKeyPollingPeriod,
		)
	}
	if pollingPeriod <= 0 {
		return 0, fmt.Errorf(
			"%q config value should be positive, got %s",
			ConfigKeyPollingPeriod,
			pollingPeriod,
		)
	}

	return pollingPeriod, nil
}

func parseMaxResults(cfgRaw map[string]string) (int32, error) {
	maxResultsString, exists := cfgRaw[ConfigKeyMaxResults]
	if !exists || maxResultsString == "" {
		return DefaultMaxResults, nil
	}

	maxResultsParsed, err := strconv.ParseInt(maxResultsString, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse %q config value: %w", ConfigKeyMaxResults, err)
	}
	if maxResultsParsed <= 0 {
		return 0, fmt.Errorf("failed to parse %q config value: value must be greater than 0, %d provided", ConfigKeyMaxResults, maxResultsParsed)
	}
	if maxResultsParsed > 5_000 {
		return 0, fmt.Errorf("failed to parse %q config value: value must not be grater than 5000, %d provided", ConfigKeyMaxResults, maxResultsParsed)
	}

	return int32(maxResultsParsed), nil
}
