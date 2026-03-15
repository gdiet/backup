# backup - Go Implementation

This backup utility uses content defined chunking together with data deduplication to efficiently backup files.

## Notes For Developers

### Start The Application

Example:

    go run ./cmd backup source target

### Code Conventions

- Use `os.Exit` only in the single exit point of `func main()`.
- Exit code `1` indicates processing failures, exit code `2` indicates incorrect usage of the application (e.g. missing arguments, invalid flags, etc.).
- Don't use `panic` outside of the non-production `assert` implementation.

### Git Hooks

To install the recommended git hooks, run

    ./build/install-git-hooks.sh

### Line Coverage in Visual Studio Code

- Uses gocover-cobertura (`go get github.com/richardlt/gocover-cobertura`)
- In VSCode, install the "Coverage Gutters" extension.
- Activate the watch mode of the extension.

`go test -coverprofile=coverage.out -covermode=atomic -coverpkg=./... ./... && go run github.com/richardlt/gocover-cobertura < coverage.out > coverage.xml`
