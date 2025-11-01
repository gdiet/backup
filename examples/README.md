````markdown
# Examples

This directory contains example code and demos for the DataStore system.

## Structure

- `datastore/` - Examples for the DataStore system

## Usage

The examples can be run directly with `go run`:

```bash
# DataStore main example
cd examples/datastore
go run tryout_filebackeddatastore.go

# Concurrency demonstration
go run concurrent_demo.go

# API performance benchmark
go run api_benchmark.go
```

## Notes

- The examples are **not part of the library** and will not be compiled by the main project
- Test files are created in `../../test-files/` (ignored by Git)
- Each example contains detailed output with explanations
- Examples demonstrate large file handling, concurrency, and performance characteristics
````
