# Go High-Performance Storage Library - Agent Onboarding Guide

## Repository Overview

Here a high-performance deduplication filesystem written in Go is developed that provides:

- Thread-safe byte store with random reads/writes at arbitrary offsets
- Handles multi-terabyte storage via 100MB file sharding (up to 1,000TB total)
- Modular design with cache, storage, metadata, and utility layers

It contains a comprehensive test suite with 99%+ coverage, race detection and benchmarks.

Performance is reached among others by:

- **Large file handling**: Via file sharding in storage (100MB+ files)
- **Concurrent access**: RWMutex-based locking for parallel reads/exclusive writes in storage
- **Caching layers**: Memory, disk, and sparse caching with area-based optimization in cache
- **Build constraints**: Debug/production builds with conditional assertions

## Build & Validation Instructions

### Core Build Commands (ALWAYS work in repository root)

1. **Basic Build** (fastest validation):

```bash
go build ./src/...
```

2. **Standard Test Suite** (main validation):

```bash
go test ./src/...
```

3. **Full Test Suite** (comprehensive):

```bash
./run-tests.sh
```

4. **Full Test Suite with Performance** (longest, includes profiling):

```bash
./run-tests.sh -all
```

### Critical Build Notes

- **Production vs Debug Builds**: The codebase uses build constraints (`//go:build !prod` vs `//go:build prod`)
- **Default is Debug Mode**: Standard commands run in debug mode where assertions panic
- **Production Build**: Use `-tags prod` for production mode where assertions log warnings
- **Test Failures in Production Mode**: Some tests WILL FAIL with `-tags prod` because they expect panics that don't occur in production builds. This is expected behavior.

### Validation Command Sequence

```bash
# 1. Format code (required before commits)
go fmt ./src/...

# 2. Lint check (must pass)
go vet ./src/...

# 3. Basic functionality test (quick validation)
go test ./src/...

# 4. Coverage test (verify comprehensive testing)
go test -cover ./src/...

# 5. Race detection (if time permits, requires CGO)
CGO_ENABLED=1 go test -race ./src/...

# 6. Benchmark tests (performance validation)
go test -bench=. -benchtime=1s ./src/...
```

### Common Issues & Workarounds

- **Race Tests May Fail**: Race detector tests can fail due to CGO issues. This is non-critical.
- **Production Test Failures**: Tests with `-tags prod` fail because assertions don't panic. This is expected.
- **Performance Tests Timeout**: Benchmarks can take 5+ seconds. Use shorter `-benchtime=500ms` if needed.
- **File Permissions**: Ensure test-files/ directory is writable (auto-created by test script).

## Project Architecture & Layout

### Source Code Structure

```
src/
├── cache/          # Multi-layer caching system (memory, disk, sparse)
│   ├── cache.go    # Main cache interface combining all layers
│   ├── memory.go   # In-memory caching with dataAreas
│   ├── disk.go     # File-backed persistent caching
│   ├── sparse.go   # Zero-filled region handling
│   └── invariants*.go # Debug validation (build constrained)
├── storage/        # Core file-backed data store
│   ├── store.go    # Main DataStore interface implementation
│   └── store_test.go # Comprehensive concurrency tests
├── metadata/       # Data serialization and tree structures
│   ├── basetypes.go # Binary serialization for dataEntry/treeEntry
│   └── basetypes_test.go # 100% coverage serialization tests
└── util/           # Assertion utilities with build constraints
    ├── assert*.go  # Debug/production assertion implementations
    └── assert_test.go # Assertion behavior tests
```

### Configuration Files

- `go.mod/go.sum`: Module definition, requires Go 1.25.2, testify dependency
- `.vscode/settings.json`: Go coverage visualization settings
- `.gitignore`: Excludes `/test-files/` and `/temp/` directories
- `run-tests.sh`: Comprehensive test runner with performance options

### Build Constraints System

The codebase uses build constraints for debug vs production behavior:

**Debug Mode (default)**:

- `assert.go` (//go:build !prod): Assertions panic to catch bugs
- `invariants.go` (//go:build !prod): Full validation enabled

**Production Mode (`-tags prod`)**:

- `assert_prod.go` (//go:build prod): Assertions log warnings
- `invariants_prod.go` (//go:build prod): Validation disabled for performance

### Key Dependencies & Patterns

- **Concurrency**: Heavy use of `sync.RWMutex` for reader/writer locks
- **File I/O**: Follows `os.File` patterns (ReadAt/WriteAt with offsets)
- **Error Handling**: Graceful degradation in production, strict validation in debug
- **Testing**: Extensive use of `stretchr/testify` for assertions
- **Serialization**: Bbolt - custom binary format with big-endian encoding

### Testing Strategy

- **Coverage Target**: 100% line coverage across all packages
- **Test Types**: Unit tests, integration tests, race condition tests, benchmarks
- **Test Data**: Generated in `test-files/` directory (git-ignored)
- **Debug Tests**: Some tests expect panics and will fail in production mode
- **Performance Tests**: Optional benchmarks with CPU/memory profiling

### Examples & Demos

Located in `examples/datastore/` (not part of main build):

```bash
cd examples/datastore
go run tryout_filebackeddatastore.go  # Basic usage demo
go run concurrent_demo.go             # Concurrency demonstration
go run api_benchmark.go               # Performance benchmarking
```

## Agent Instructions

1. **Always run from repository root**
2. **Use `go test ./src/...`** for quick validation after changes
3. **Run `./run-tests.sh`** for comprehensive validation before major changes
4. **Expect production test failures** - this is normal behavior
5. **Focus on `src/` directory** - examples are standalone demos
6. **Follow build constraint patterns** when adding new features
7. **Maintain 100% test coverage** - add tests for any new code
8. **Use RWMutex patterns** for any new concurrent code
9. **Follow `os.File` API consistency** for I/O operations
10. **Trust these instructions** - search/explore only if information is incomplete or incorrect

## Code Style, Patterns & Idioms

- File I/O API Consistency: Follow os.File patterns (ReadAt/WriteAt with offsets)
- Pre-allocate slices when size is known
- Avoid allocations in hot paths
- Memory allocations: Minimize garbage collection pressure
- Error Handling:
  - Debug: Panic on assertion failures to catch bugs early
  - Production: Log warnings and continue with fallbacks

## Suggested Improvements & Patterns

When working on this codebase, consider:

- Lazy evaluation for expensive computations in debug paths
- Interface segregation for better testability
- Graceful degradation in error scenarios
- Consistent naming across similar operations
- Documentation of concurrency guarantees and invariants
