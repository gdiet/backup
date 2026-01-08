#!/bin/bash

# Generiere Coverage Report
go test -coverprofile=coverage.out -covermode=atomic -coverpkg=./... ./...

# Generiere HTML Report
go tool cover -html=coverage.out -o coverage.html

# Konvertiere zu Light Theme
sed -i 's/background: black/background: white/g' coverage.html
sed -i 's/border-bottom: 1px solid rgb(80, 80, 80)/border-bottom: 1px solid #ddd/g' coverage.html
sed -i '/#topbar {/,/}/ s/background: black/background: #f8f8f8/' coverage.html

echo "Light theme coverage report generated: coverage.html"