### bbolt Buckets For The File System Metadata

#### General Format

- Numbers (id, start, length) are 8-byte big endian uint64 values.
- hash is the 32-byte Blake3 hash.
- File system settings are persisted as key-value pairs.

#### Buckets:

| Bucket             | Key                    | Value                   |
| ------------------ | ---------------------- | ----------------------- |
| tree entries       | id                     | dir entry or file entry |
| children           | parent id and child id | ---                     |
| data entries       | length and hash        | list of start and end   |
| free areas         | start                  | length                  |
| settings (planned) | string                 | string                  |

#### Dir Entry:

| Field     | Size (bytes) | Description          |
| --------- | ------------ | -------------------- |
| type      | 1            | entry type (value 0) |
| name      | average 23   | file name (UTF-8)    |
| **total** | 24           |                      |

#### File Entry:

| Field          | Size (bytes) | Description             |
| -------------- | ------------ | ----------------------- |
| type           | 1            | entry type (value 1)    |
| last modified  | 8            | unix times milliseconds |
| data reference | 8 + 32 = 40  | length and hash         |
| name           | average 23   | file name (UTF-8)       |
| **total**      | 72           |                         |

#### Estimated Metadata Size of a Single File:

| Field            | Size (bytes) |
| ---------------- | ------------ |
| tree entry id    | 8            |
| file entry       | 72           |
| children         | 16           |
| data entry key   | 40           |
| data entry value | 16           |
| bbolt overhead   | 48           |
| **total**        | 200          |
