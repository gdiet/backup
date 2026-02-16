### Line Coverage in Visual Studio Code

- Uses gocover-cobertura (`go get github.com/richardlt/gocover-cobertura`)
- In VSCode, install the "Coverage Gutters" extension.
- Activate the watch mode of the extension.

`go test -coverprofile=coverage.out -covermode=atomic -coverpkg=./... ./... && go run github.com/richardlt/gocover-cobertura < coverage.out > coverage.xml`
