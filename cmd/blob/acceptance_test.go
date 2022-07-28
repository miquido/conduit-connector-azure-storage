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

package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	sdk "github.com/conduitio/conduit-connector-sdk"
	as "github.com/miquido/conduit-connector-azure-storage"
	asSource "github.com/miquido/conduit-connector-azure-storage/source"
	helper "github.com/miquido/conduit-connector-azure-storage/test"
	"go.uber.org/goleak"
)

type CustomConfigurableAcceptanceTestDriver struct {
	sdk.ConfigurableAcceptanceTestDriver

	containerClient *azblob.ContainerClient
}

func (d *CustomConfigurableAcceptanceTestDriver) GenerateRecord(t *testing.T) sdk.Record {
	record := d.ConfigurableAcceptanceTestDriver.GenerateRecord(t)

	// Override Key
	record.Key = sdk.RawData(fmt.Sprintf("file-%d.txt", time.Now().UnixMicro()))

	// Override CreatedAt
	// azurite does not support milliseconds precision
	record.CreatedAt = record.CreatedAt.Truncate(time.Second)

	return record
}

func (d *CustomConfigurableAcceptanceTestDriver) WriteToSource(t *testing.T, records []sdk.Record) (results []sdk.Record) {
	for _, record := range records {
		_ = helper.CreateBlob(
			d.containerClient,
			string(record.Key.Bytes()),
			"text/plain",
			string(record.Payload.Bytes()),
		)
	}

	// No destination connector, return wanted records
	return records
}

func TestAcceptance(t *testing.T) {
	sourceConfig := map[string]string{
		asSource.ConfigKeyConnectionString: helper.GetConnectionString(),
		asSource.ConfigKeyContainerName:    "acceptance-tests",
	}

	testDriver := CustomConfigurableAcceptanceTestDriver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector: sdk.Connector{
					NewSpecification: as.Specification,
					NewSource:        asSource.NewSource,
					NewDestination:   nil,
				},

				SourceConfig:     sourceConfig,
				GenerateDataType: sdk.GenerateRawData,

				GoleakOptions: []goleak.Option{
					// Routines created by Azure client
					goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
					goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
				},

				Skip: []string{
					// Cannot be tested. driver.WriteToSource() creates all records at once while CDC position is
					// created after the Snapshot iterator finishes iterating. So from the CDC iterator point of view,
					// there are no new records available.
					"TestAcceptance/TestSource_Open_ResumeAtPositionCDC",

					// Same as above: due to the nature of the CDC iterator, which runs after snapshot iterator,
					// there are no changes left to detect.
					// Only `TestAcceptance/TestSource_Read_Success/cdc` fails, but it cannot be excluded alone.
					"TestAcceptance/TestSource_Read_Success",
				},
			},
		},
	}

	testDriver.ConfigurableAcceptanceTestDriver.Config.BeforeTest = func(t *testing.T) {
		testDriver.containerClient = helper.PrepareContainer(t, helper.NewAzureBlobServiceClient(), sourceConfig[asSource.ConfigKeyContainerName])
	}

	sdk.AcceptanceTest(t, &testDriver)
}
