Package rules for metadata/internal:

- IDs are []byte -8 bytes for uint64
- no assertions, telling typed errors instead
- 100% target test coverage
- no nil checks for \*bbolt.Bucket
- all reads can return DeserializationError
