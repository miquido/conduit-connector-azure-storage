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
	"errors"
	"io/ioutil"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/miquido/conduit-connector-azure-storage/source"
	"gopkg.in/tomb.v2"
)

func NewCDCIterator(
	pollingPeriod time.Duration,
	client *azblob.ContainerClient,
	from time.Time,
) (*CDCIterator, error) {
	cdc := CDCIterator{
		client:        client,
		buffer:        make(chan sdk.Record, 1),
		ticker:        time.NewTicker(pollingPeriod),
		isTruncated:   true,
		nextKeyMarker: nil,
		tomb:          tomb.Tomb{},
		lastModified:  from,
	}

	cdc.tomb.Go(cdc.producer)

	return &cdc, nil
}

type CDCIterator struct {
	client        *azblob.ContainerClient
	buffer        chan sdk.Record
	ticker        *time.Ticker
	lastModified  time.Time
	isTruncated   bool
	nextKeyMarker *string
	tomb          tomb.Tomb
}

func (w *CDCIterator) HasNext(_ context.Context) bool {
	return true
	// return len(w.buffer) > 0 || !w.tomb.Alive() // if tomb is dead we return true so caller will fetch error with Next
}

func (w *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case r := <-w.buffer:
		return r, nil

	case <-w.tomb.Dead():
		return sdk.Record{}, w.tomb.Err()

	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	}
}

func (w *CDCIterator) Stop() {
	w.ticker.Stop()
	w.tomb.Kill(errors.New("CDC iterator is stopped"))
}

func (w *CDCIterator) producer() error {
	defer close(w.buffer)

	for {
		select {
		case <-w.tomb.Dying():
			return w.tomb.Err()

		case <-w.ticker.C:
			currentLastModified := w.lastModified

			var maxResults int32 = 5

			blobListPager := w.client.ListBlobsFlat(&azblob.ContainerListBlobsFlatOptions{
				Marker:     w.nextKeyMarker,
				MaxResults: &maxResults,
				Include: []azblob.ListBlobsIncludeItem{
					azblob.ListBlobsIncludeItemDeleted,
					azblob.ListBlobsIncludeItemSnapshots,
					azblob.ListBlobsIncludeItemVersions,
					azblob.ListBlobsIncludeItemTags,
				},
			})

			for blobListPager.NextPage(w.tomb.Context(context.TODO())) {
				resp := blobListPager.PageResponse()

				for _, item := range resp.Segment.BlobItems {
					itemLastModificationDate := *item.Properties.LastModified

					if itemLastModificationDate.Before(w.lastModified) {
						continue
					}

					var output sdk.Record

					if nil != item.Deleted && *item.Deleted {
						output = w.createDeletedRecord(item)
					} else {
						blobClient, err := w.client.NewBlobClient(*item.Name)
						if err != nil {
							return err
						}

						downloadResponse, err := blobClient.Download(w.tomb.Context(context.TODO()), nil)
						if err != nil {
							return err
						}

						output, err = w.createUpsertedRecord(item, downloadResponse)
						if err != nil {
							return err
						}
					}

					select {
					case w.buffer <- output:
						if currentLastModified.Before(itemLastModificationDate) {
							currentLastModified = itemLastModificationDate
						}

					case <-w.tomb.Dying():
						return w.tomb.Err()
					}
				}
			}

			w.lastModified = currentLastModified.Add(time.Nanosecond)

			if err := blobListPager.Err(); err != nil {
				return err
			}
		}
	}
}

func (w *CDCIterator) createUpsertedRecord(entry *azblob.BlobItemInternal, object azblob.BlobDownloadResponse) (sdk.Record, error) {
	rawBody, err := ioutil.ReadAll(object.Body(&azblob.RetryReaderOptions{
		MaxRetryRequests: 0,
	}))
	if err != nil {
		return sdk.Record{}, err
	}

	p := source.Position{
		Key:       *entry.Name,
		Timestamp: *entry.Properties.LastModified,
		Type:      source.TypeCDC,
	}

	return sdk.Record{
		Metadata: map[string]string{
			"content-type": *object.ContentType,
		},
		Position:  p.ToRecordPosition(),
		Payload:   sdk.RawData(rawBody),
		Key:       sdk.RawData(p.Key),
		CreatedAt: p.Timestamp,
	}, nil
}

func (w *CDCIterator) createDeletedRecord(entry *azblob.BlobItemInternal) sdk.Record {
	p := source.Position{
		Key:       *entry.Name,
		Timestamp: *entry.Properties.LastModified,
		Type:      source.TypeCDC,
	}

	return sdk.Record{
		Metadata: map[string]string{
			"action": "delete",
		},
		Position:  p.ToRecordPosition(),
		Key:       sdk.RawData(p.Key),
		CreatedAt: p.Timestamp,
	}
}
