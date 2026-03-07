# backup - Go Implementation

This backup utility uses content defined chunking together with data deduplication to efficiently backup files.

## Notes For Developers

### Start The Application

Example:

`go run ./cmd backup source target`

### Line Coverage in Visual Studio Code

- Uses gocover-cobertura (`go get github.com/richardlt/gocover-cobertura`)
- In VSCode, install the "Coverage Gutters" extension.
- Activate the watch mode of the extension.

`go test -coverprofile=coverage.out -covermode=atomic -coverpkg=./... ./... && go run github.com/richardlt/gocover-cobertura < coverage.out > coverage.xml`
