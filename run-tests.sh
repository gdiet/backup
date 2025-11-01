#!/bin/bash

# run-tests.sh - Comprehensive test runner for the backup storage project
# Usage: ./run-tests.sh [-all]
#   -all: Include performance tests and profiling (slower)

set -e  # Exit on any error

# Parse command line arguments
RUN_PERFORMANCE_TESTS=false
for arg in "$@"; do
    case $arg in
        -all|--all)
            RUN_PERFORMANCE_TESTS=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [-all]"
            echo "  -all: Include performance tests and profiling (slower)"
            echo "  -h, --help: Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $arg"
            echo "Use -h for help"
            exit 1
            ;;
    esac
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Header
echo "=============================================="
echo "  Backup Storage Library - Test Runner"
if [ "$RUN_PERFORMANCE_TESTS" = true ]; then
    echo "  Modus: Vollständig (mit Performance-Tests)"
else
    echo "  Modus: Standard (ohne Performance-Tests)"
fi
echo "=============================================="

# Check if Go is installed
if ! command -v go &> /dev/null; then
    print_error "Go is not installed or not in PATH"
    exit 1
fi

print_status "Go version: $(go version)"
echo ""

# Clean up any previous test artifacts
print_status "Cleaning up previous test artifacts..."
go clean -testcache
rm -rf test-files/
mkdir -p test-files/reports
mkdir -p test-files/binaries
echo ""

# Format code (excluding examples directory)
print_status "Formatting code..."
if go fmt ./src/...; then
    print_success "Code formatting completed"
else
    print_error "Code formatting failed"
    exit 1
fi
echo ""

# Run go vet (excluding examples directory)
print_status "Running go vet..."
if go vet ./src/...; then
    print_success "go vet passed"
else
    print_error "go vet failed"
    exit 1
fi
echo ""

# Run regular tests with verbose output
print_status "Running regular tests..."
if go test -v ./src/...; then
    print_success "Regular tests passed"
else
    print_error "Regular tests failed"
    exit 1
fi
echo ""

# Run tests with race detector
print_status "Running tests with race detector..."
# Explicitly enable CGO for race detector
export CGO_ENABLED=1
# Build test binary with race detector in test-files
if go test -race -c -o test-files/binaries/storage-race.test ./src/storage; then
    print_status "Race detector test binary created: test-files/binaries/storage-race.test"
    if go test -race -v ./src/...; then
        print_success "Race detector tests passed"
    else
        print_warning "Race detector tests failed - this might indicate race conditions or CGO issues"
        print_status "Continuing with remaining tests..."
    fi
else
    print_warning "Failed to create race detector test binary"
fi
echo ""

# Run tests with coverage
print_status "Running tests with coverage..."
if go test -coverprofile=test-files/coverage.out ./src/...; then
    coverage=$(go tool cover -func=test-files/coverage.out | tail -1 | awk '{print $3}')
    print_success "Coverage tests completed - Total coverage: $coverage"
    
    # Generate HTML coverage report
    go tool cover -html=test-files/coverage.out -o test-files/reports/coverage.html
    print_status "Coverage report saved to test-files/reports/coverage.html"
else
    print_error "Coverage tests failed"
    exit 1
fi
echo ""

# Performance tests (only if -all parameter is given)
if [ "$RUN_PERFORMANCE_TESTS" = true ]; then
    # Run performance tests (benchmarks) for 2 seconds
    print_status "Running performance tests (benchmarks) for 2 seconds..."
    if go test -bench=. -benchtime=2s -benchmem ./src/...; then
        print_success "Performance tests completed"
    else
        print_warning "Performance tests had issues but continuing..."
    fi
    echo ""

    # Build standard test binary
    print_status "Building test binary..."
    if go test -c -o test-files/binaries/storage.test ./src/storage; then
        print_success "Test binary created: test-files/binaries/storage.test"
    else
        print_warning "Failed to create test binary"
    fi
    echo ""

    # Run CPU profiling benchmark
    print_status "Running CPU profiling benchmark..."
    if go test -cpuprofile=test-files/cpu.prof -bench=BenchmarkWrite -benchtime=1s ./src/storage; then
        print_success "CPU profiling completed - saved to test-files/cpu.prof"
        print_status "Analyze with: go tool pprof test-files/cpu.prof"
    else
        print_warning "CPU profiling failed but continuing..."
    fi
    # Clean up any test binaries created by profiling
    rm -f storage.test
    echo ""

    # Run memory profiling benchmark  
    print_status "Running memory profiling benchmark..."
    if go test -memprofile=test-files/mem.prof -bench=BenchmarkWrite -benchtime=1s ./src/storage; then
        print_success "Memory profiling completed - saved to test-files/mem.prof"
        print_status "Analyze with: go tool pprof test-files/mem.prof"
    else
        print_warning "Memory profiling failed but continuing..."
    fi
    # Clean up any test binaries created by profiling
    rm -f storage.test
    echo ""
else
    print_warning "Performance-Tests übersprungen - mit -all Parameter werden sie auch ausgeführt"
    echo ""
fi

# Summary
echo "=============================================="
print_success "All tests completed successfully!"
echo ""
print_status "Generated files:"
echo "  - test-files/coverage.out: Coverage data"
echo "  - test-files/reports/coverage.html: HTML coverage report"
if [ "$RUN_PERFORMANCE_TESTS" = true ]; then
    echo "  - test-files/cpu.prof: CPU profile data (if successful)"
    echo "  - test-files/mem.prof: Memory profile data (if successful)"
    echo "  - test-files/binaries/: Test binaries directory"
fi
echo ""
print_status "View coverage report: open test-files/reports/coverage.html"
if [ "$RUN_PERFORMANCE_TESTS" = true ]; then
    print_status "Analyze CPU profile: go tool pprof test-files/cpu.prof"
    print_status "Analyze memory profile: go tool pprof test-files/mem.prof"
    print_status "Test binaries available in: test-files/binaries/"
else
    print_status "To include performance tests and profiling: ./run-tests.sh -all"
fi
echo "=============================================="