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

package iterator

import (
	"context"
	"io/ioutil"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/miquido/conduit-connector-azure-storage/source"
	"gopkg.in/tomb.v2"
)

type SnapshotIterator struct {
	client          *azblob.ContainerClient
	paginator       *azblob.ContainerListBlobFlatPager
	maxLastModified time.Time
	buffer          chan sdk.Record
	finished        bool
	tomb            tomb.Tomb
}

func NewSnapshotIterator(client *azblob.ContainerClient, p source.Position, maxResults int32) (*SnapshotIterator, error) {
	iterator := SnapshotIterator{
		client: client,
		paginator: client.ListBlobsFlat(&azblob.ContainerListBlobsFlatOptions{
			MaxResults: &maxResults,
			Include: []azblob.ListBlobsIncludeItem{
				// azblob.ListBlobsIncludeItemDeleted,
				azblob.ListBlobsIncludeItemSnapshots,
				azblob.ListBlobsIncludeItemVersions,
				// azblob.ListBlobsIncludeItemTags,
			},
		}),
		maxLastModified: p.Timestamp,
		buffer:          make(chan sdk.Record, 1),
		finished:        false,
		tomb:            tomb.Tomb{},
	}

	iterator.tomb.Go(iterator.producer)

	return &iterator, nil
}

func (w *SnapshotIterator) HasNext(_ context.Context) bool {
	return !w.finished && w.tomb.Alive()
}

func (w *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case r := <-w.buffer:
		return r, nil

	case <-w.tomb.Dead():
		return sdk.Record{}, w.tomb.Err()

	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	}
}

func (w *SnapshotIterator) Stop() {
	_ = w.tomb.Killf("snapshot iterator is stopped")
}

func (w *SnapshotIterator) producer() error {
	defer func() {
		close(w.buffer)

		w.finished = true
	}()

worker:
	for {
		select {
		case <-w.tomb.Dying():
			return w.tomb.Err()

		default:
			if w.paginator.NextPage(w.tomb.Context(context.TODO())) {
				resp := w.paginator.PageResponse()

				for _, item := range resp.Segment.BlobItems {
					if w.maxLastModified.Before(*item.Properties.LastModified) {
						w.maxLastModified = *item.Properties.LastModified
					}

					blobClient, err := w.client.NewBlobClient(*item.Name)
					if err != nil {
						return err
					}

					downloadResponse, err := blobClient.Download(w.tomb.Context(context.TODO()), nil)
					if err != nil {
						return err
					}

					rawBody, err := ioutil.ReadAll(downloadResponse.Body(&azblob.RetryReaderOptions{
						MaxRetryRequests: 0,
					}))
					if err != nil {
						return err
					}

					p := source.Position{
						Key:       *item.Name,
						Type:      source.TypeSnapshot,
						Timestamp: w.maxLastModified,
					}

					record := sdk.Record{
						Metadata: map[string]string{
							"content-type": *item.Properties.ContentType,
						},
						Position:  p.ToRecordPosition(),
						Payload:   sdk.RawData(rawBody),
						Key:       sdk.RawData(*item.Name),
						CreatedAt: *item.Properties.CreationTime,
					}

					select {
					case w.buffer <- record:
						//

					case <-w.tomb.Dying():
						return w.tomb.Err()
					}
				}

				continue
			}

			if err := w.paginator.Err(); err != nil {
				return err
			}

			break worker
		}
	}

	return nil
}
