# Conduit Connector Azure Storage

## General

The Azure Storage plugin is one of [Conduit](https://github.com/ConduitIO/conduit) plugins.
It currently provides only source connector.

## How to build it

Run `make`.

## Source

The Source connector monitors given Azure Blob container for file changes and generates appropriate Record when change is detected.
It supports two reading modes and switches them automatically:
- Snapshot
- CDC

In **Snapshot mode**, connector reads the current state of the container, meaning it does not include changes made during this process.

After Snapshot reading is finished, connector switches to **CDC mode**. In this mode, connector monitors the container each `pollingPeriod` period and notifies about changes detected.

### Supported storage changes

Changes regarding adding new files to the storage or updating the existing ones are always detected.
However, [soft delete for blobs](https://docs.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-enable) needs to be enabled to detect deleted files.

### Configuration Options

| name               | description                                                                                                                                  | required | default  |
|--------------------|----------------------------------------------------------------------------------------------------------------------------------------------|----------|----------|
| `connectionString` | Azure Storage connection string as described here: https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string | `true`   |          |
| `containerName`    | The name of the container to monitor.                                                                                                        | `true`   |          |
| `pollingPeriod`    | The polling period for the CDC mode, formatted as a time.Duration string. Must be greater then `0`.                                          | `false`  | `"1s"`   |
| `maxResults`       | The maximum number of items, per page, when reading container's items. The minimum value is `1`, maximum value is `5000`.                    | `false`  | `"5000"` |

## Testing

Run `make test` to run all the unit and integration tests, which require Docker to be installed and running. The command
will handle starting and stopping docker containers for you.

## References

- [What is Azure Blob storage?](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blobs-overview)
- [Configure Azure Storage connection strings](https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string)
- [Dev] [Use the Azurite emulator for local Azure Storage development](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azurite)
- [Dev] [Microsoft client tools for working with Azure Storage](https://docs.microsoft.com/en-us/azure/storage/common/storage-explorers)
