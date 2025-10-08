# DataStore Examples

Examples and demos for the FileBackedDataStore system.

## tryout_filebackeddatastore.go

Demonstrates the functionality of FileBackedDataStore with different offset sizes:

- Small offsets (0, 100MB, 1.2GB)
- Medium offsets (33GB, 456GB)
- Large offsets (21TB, 987TB)

### Execution

```bash
cd examples/datastore
go run tryout_filebackeddatastore.go
```

### What is tested

1. **Creation** of DataStore with 100MB fileSize
2. **Writing** test data at different offsets
3. **Directory structure analysis** of created files
4. **Data verification** by reading back
5. **FileID calculation** and path mapping

### Output

The example creates a detailed analysis of:

- FileID calculations
- Directory structure (2-level hierarchy)
- File system layout with sparse file support
- Performance characteristics for different file sizes

### Test Files

Test files are created in `../../test-files/examples_datastore/` and can be manually inspected.
