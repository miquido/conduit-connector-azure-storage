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

//go:build integration

package source

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jaswdr/faker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSource(t *testing.T) {
	fakerInstance := faker.New()

	var (
		accountName      = "devstoreaccount1"
		accountKey       = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
		blobEndpoint     = fmt.Sprintf("http://127.0.0.1:10000/%s", accountName)
		connectionString = fmt.Sprintf(
			"DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s;BlobEndpoint=%s",
			"http",
			accountName,
			accountKey,
			blobEndpoint,
		)
		containerName = "foo"

		cfgRaw = map[string]string{
			ConfigKeyConnectionString: connectionString,
			ConfigKeyContainerName:    containerName,
			ConfigKeyPollingPeriod:    "1500ms",
			ConfigKeyMaxResults:       "1",
		}
	)

	ctx := context.Background()

	serviceClient, err := azblob.NewServiceClientFromConnectionString(connectionString, nil)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = serviceClient.DeleteContainer(ctx, containerName, nil)
	})

	_, err = serviceClient.CreateContainer(ctx, containerName, nil)
	require.NoError(t, err)

	containerClient, err := serviceClient.NewContainerClient(containerName)
	require.NoError(t, err)

	var (
		alreadyExistingBlob1Name        = "already-existing-1.txt"
		alreadyExistingBlob1ContentType = "text/plain; charset=utf-8"
		alreadyExistingBlob1Contents    = fakerInstance.Lorem().Sentence(16)

		alreadyExistingBlob2Name        = "already-existing-2.txt"
		alreadyExistingBlob2ContentType = "text/plain; charset=utf-8"
		alreadyExistingBlob2Contents    = fakerInstance.Lorem().Sentence(16)

		createdWhileWorking1Name        = "created-while-running-1.txt"
		createdWhileWorking1ContentType = "text/plain; charset=utf-8"
		createdWhileWorking1Contents    = fakerInstance.Lorem().Sentence(16)
	)

	_, _, _ = createdWhileWorking1Name, createdWhileWorking1ContentType, createdWhileWorking1Contents

	createBlob(t, ctx, containerClient, alreadyExistingBlob1Name, alreadyExistingBlob1ContentType, alreadyExistingBlob1Contents)
	createBlob(t, ctx, containerClient, alreadyExistingBlob2Name, alreadyExistingBlob2ContentType, alreadyExistingBlob2Contents)

	time.Sleep(time.Second)

	src := NewSource()

	require.NoError(t, src.Configure(ctx, cfgRaw))
	require.NoError(t, src.Open(ctx, nil))

	t.Cleanup(func() {
		_ = src.Teardown(ctx)
	})

	t.Run("Firstly, snapshot iterator runs and reads all blobs", func(t *testing.T) {
		// Read the first record from the first page
		rerecord1, err := src.Read(ctx)
		require.NoError(t, err)
		require.True(t, assertRecordEquals(t, rerecord1, alreadyExistingBlob1Name, alreadyExistingBlob1ContentType, alreadyExistingBlob1Contents))
		require.NoError(t, src.Ack(ctx, rerecord1.Position))

		// Read the second record from the second page
		rerecord2, err := src.Read(ctx)
		require.NoError(t, err)
		require.True(t, assertRecordEquals(t, rerecord2, alreadyExistingBlob2Name, alreadyExistingBlob2ContentType, alreadyExistingBlob2Contents))
		require.NoError(t, src.Ack(ctx, rerecord2.Position))

		// No third record available, backoff
		// By this time, combined iterator should switch from snapshot to CDC iterator
		rerecord3, err := src.Read(ctx)
		require.Equal(t, sdk.Record{}, rerecord3)
		require.ErrorIs(t, err, sdk.ErrBackoffRetry)
	})

	t.Run("Secondly, CDC iterator runs and reads all blobs created meanwhile", func(t *testing.T) {
		// Create file while snapshot iterator is working
		// This file should not be included in the results
		createBlob(t, ctx, containerClient, createdWhileWorking1Name, createdWhileWorking1ContentType, createdWhileWorking1Contents)

		// No new record is available until 1 polling period passes
		rerecord1, err := src.Read(ctx)
		require.Equal(t, sdk.Record{}, rerecord1)
		require.ErrorIs(t, err, sdk.ErrBackoffRetry)

		// Polling Period is 1.5s
		time.Sleep(time.Second * 2)

		// Read the first record polled
		rerecord2, err := src.Read(ctx)
		require.NoError(t, err)
		require.True(t, assertRecordEquals(t, rerecord2, createdWhileWorking1Name, createdWhileWorking1ContentType, createdWhileWorking1Contents))
		require.NoError(t, src.Ack(ctx, rerecord2.Position))

		// Read the second record polled
		rerecord3, err := src.Read(ctx)
		require.Equal(t, sdk.Record{}, rerecord3)
		require.ErrorIs(t, err, sdk.ErrBackoffRetry)
	})
}

func createBlob(
	t *testing.T,
	ctx context.Context,
	containerClient *azblob.ContainerClient,
	blobName, contentType, contents string,
) {
	blockBlobClient, err := containerClient.NewBlockBlobClient(blobName)
	require.NoError(t, err)

	requestBody := streaming.NopCloser(strings.NewReader(contents))
	_, err = blockBlobClient.Upload(ctx, requestBody, &azblob.BlockBlobUploadOptions{
		HTTPHeaders: &azblob.BlobHTTPHeaders{
			BlobContentType:        to.Ptr(contentType),
			BlobContentDisposition: to.Ptr("attachment"),
		},
	})
	require.NoError(t, err)
}

func assertRecordEquals(t *testing.T, read sdk.Record, fileName, contentType, contents string) bool {
	return assert.Equal(t, fileName, string(read.Key.Bytes()), "Record name does not match.") &&
		assert.Equal(t, contentType, read.Metadata["content-type"], "Record's content-type metadata does not match.") &&
		assert.Equal(t, contents, string(read.Payload.Bytes()), "Record payload does not match.")
}
