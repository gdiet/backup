#!/bin/bash

# create coverage report
go test -coverprofile=coverage.out -covermode=atomic -coverpkg=./... ./...

# convert to Cobertura format for Coverage Gutters extension
go run github.com/richardlt/gocover-cobertura < coverage.out > coverage.xml

# generate HTML report
go tool cover -html=coverage.out -o coverage.html

# convert to Light Theme
sed -i 's/background: black/background: white/g' coverage.html
sed -i 's/border-bottom: 1px solid rgb(80, 80, 80)/border-bottom: 1px solid #ddd/g' coverage.html
sed -i '/#topbar {/,/}/ s/background: black/background: #f8f8f8/' coverage.html

echo "Light theme coverage report generated: coverage.html"
