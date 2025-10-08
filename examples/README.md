# Examples

This directory contains example code and demos for the various components of the backup system.

## Structure

- `datastore/` - Examples for the DataStore system
- `cache/` - Examples for the caching system (planned)

## Usage

The examples can be run directly with `go run`:

```bash
# DataStore example
cd examples/datastore
go run tryout_filebackeddatastore.go
```

## Notes

- The examples are **not part of the library** and will not be compiled
- Test files are created in `../../test-files/` (ignored by Git)
- Each example contains detailed output with explanations
