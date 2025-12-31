### Prerequisites

- Linux build: `sudo apt install fuse libfuse-dev gcc` (and CGO_ENABLED=1 ???)
- Linux runtime: `sudo apt install fuse` ???
- Windows: Install WinFSP

### Starting The Filesystem

- Linux: The mount point must be an existing empty directory.
- Windows: The mount point must be either a non-existing entry in an existing directory or a drive letter (e.g., `X:`).

Relative paths are supported for the mount point.

```
go run ./src/. <mount-point>
```

### Running All Tests

```
go test -v ./src/...
```

### Line Coverage in Visual Studio Code

- If necessary: `go get github.com/richardlt/gocover-cobertura`
- Install the "Coverage Gutters" extension.
- Activate the watch mode of the extension.

```
go test -v -coverprofile=coverage.out -covermode=atomic -coverpkg=./... ./... && go run github.com/richardlt/gocover-cobertura < coverage.out > coverage.xml
```
