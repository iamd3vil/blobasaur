#!/bin/bash

# Blobnom Benchmarking Suite
# This script benchmarks the Blobnom server using multiple tools and approaches

set -e

# Configuration
HOST="127.0.0.1"
PORT="6380"
DURATION="30s"
CONNECTIONS="50"
PIPELINE="10"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if server is running
check_server() {
    log_info "Checking if Blobnom server is running on $HOST:$PORT..."
    if ! nc -z $HOST $PORT 2>/dev/null; then
        log_error "Server is not running on $HOST:$PORT"
        log_info "Please start the server with: cargo run --release"
        exit 1
    fi
    log_success "Server is running"
}

# Test basic connectivity
test_connectivity() {
    log_info "Testing basic connectivity..."
    echo "PING" | nc $HOST $PORT | grep -q "PONG" && log_success "PING test passed" || log_error "PING test failed"
}

# Run redis-benchmark if available
run_redis_benchmark() {
    if command -v redis-benchmark &> /dev/null; then
        log_info "Running redis-benchmark tests..."

        echo -e "\n${YELLOW}=== Redis-benchmark: SET operations ===${NC}"
        redis-benchmark -h $HOST -p $PORT -t set -n 10000 -c $CONNECTIONS -d 100 --csv

        echo -e "\n${YELLOW}=== Redis-benchmark: GET operations ===${NC}"
        redis-benchmark -h $HOST -p $PORT -t get -n 10000 -c $CONNECTIONS -d 100 --csv

        echo -e "\n${YELLOW}=== Redis-benchmark: Mixed operations ===${NC}"
        redis-benchmark -h $HOST -p $PORT -t set,get -n 10000 -c $CONNECTIONS -d 1000 --csv

        echo -e "\n${YELLOW}=== Redis-benchmark: Large payloads (10KB) ===${NC}"
        redis-benchmark -h $HOST -p $PORT -t set,get -n 1000 -c 20 -d 10240 --csv

        log_success "redis-benchmark completed"
    else
        log_warning "redis-benchmark not found, skipping..."
    fi
}

# Run valkey-benchmark if available
run_valkey_benchmark() {
    if command -v valkey-benchmark &> /dev/null; then
        log_info "Running valkey-benchmark tests..."

        echo -e "\n${YELLOW}=== Valkey-benchmark: SET operations ===${NC}"
        valkey-benchmark -h $HOST -p $PORT -t set -n 10000 -c $CONNECTIONS -d 100 --csv

        echo -e "\n${YELLOW}=== Valkey-benchmark: GET operations ===${NC}"
        valkey-benchmark -h $HOST -p $PORT -t get -n 10000 -c $CONNECTIONS -d 100 --csv

        echo -e "\n${YELLOW}=== Valkey-benchmark: Mixed operations ===${NC}"
        valkey-benchmark -h $HOST -p $PORT -t set,get -n 10000 -c $CONNECTIONS -d 1000 --csv

        log_success "valkey-benchmark completed"
    else
        log_warning "valkey-benchmark not found, skipping..."
    fi
}



# Run memtier benchmark if available
run_memtier_benchmark() {
    if command -v memtier_benchmark &> /dev/null; then
        log_info "Running memtier_benchmark tests..."

        echo -e "\n${YELLOW}=== Memtier: Mixed workload ===${NC}"
        memtier_benchmark -s $HOST -p $PORT --protocol=redis \
            --clients=10 --threads=4 --requests=1000 \
            --data-size=100 --key-pattern=R:R --ratio=1:1

        echo -e "\n${YELLOW}=== Memtier: Write-heavy workload ===${NC}"
        memtier_benchmark -s $HOST -p $PORT --protocol=redis \
            --clients=10 --threads=4 --requests=1000 \
            --data-size=1000 --key-pattern=R:R --ratio=3:1

        log_success "memtier_benchmark completed"
    else
        log_warning "memtier_benchmark not found, skipping..."
        log_info "Install with: brew install memtier-benchmark (macOS) or apt-get install memtier-benchmark (Ubuntu)"
    fi
}

# Generate test data
generate_test_data() {
    log_info "Generating test data files..."

    # Create various sized test files
    dd if=/dev/urandom of=test_1kb.bin bs=1024 count=1 2>/dev/null
    dd if=/dev/urandom of=test_10kb.bin bs=10240 count=1 2>/dev/null
    dd if=/dev/urandom of=test_100kb.bin bs=102400 count=1 2>/dev/null

    log_success "Test data files generated"
}

# Custom benchmark using redis-cli
run_redis_cli_benchmark() {
    if command -v redis-cli &> /dev/null; then
        log_info "Running custom redis-cli tests..."

        echo -e "\n${YELLOW}=== Testing binary data storage ===${NC}"

        # Test different data sizes
        for size in "1kb" "10kb" "100kb"; do
            echo "Testing ${size} binary data..."

            # SET operation
            start_time=$(date +%s.%N)
            redis-cli -h $HOST -p $PORT --raw SET "test_${size}" "$(cat test_${size}.bin)" > /dev/null
            set_time=$(echo "$(date +%s.%N) - $start_time" | bc)

            # GET operation
            start_time=$(date +%s.%N)
            redis-cli -h $HOST -p $PORT --raw GET "test_${size}" > /dev/null
            get_time=$(echo "$(date +%s.%N) - $start_time" | bc)

            # DEL operation
            start_time=$(date +%s.%N)
            redis-cli -h $HOST -p $PORT DEL "test_${size}" > /dev/null
            del_time=$(echo "$(date +%s.%N) - $start_time" | bc)

            printf "%-8s SET: %8.3fms  GET: %8.3fms  DEL: %8.3fms\n" \
                "$size" \
                "$(echo "$set_time * 1000" | bc)" \
                "$(echo "$get_time * 1000" | bc)" \
                "$(echo "$del_time * 1000" | bc)"
        done

        log_success "redis-cli benchmark completed"
    else
        log_warning "redis-cli not found, skipping custom tests..."
    fi
}

# System information
show_system_info() {
    echo -e "\n${BLUE}=== System Information ===${NC}"
    echo "OS: $(uname -s) $(uname -r)"
    echo "CPU: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep 'model name' /proc/cpuinfo | head -1 | cut -d':' -f2 | xargs || echo 'Unknown')"
    echo "Memory: $(free -h 2>/dev/null | grep Mem | awk '{print $2}' || sysctl -n hw.memsize | awk '{print $1/1024/1024/1024 " GB"}' || echo 'Unknown')"
    echo "Target: $HOST:$PORT"
    echo "Date: $(date)"
}

# Main execution
main() {
    echo -e "${GREEN}"
    echo "╔══════════════════════════════════════╗"
    echo "║        Blobnom Benchmark Suite       ║"
    echo "╚══════════════════════════════════════╝"
    echo -e "${NC}"

    show_system_info
    check_server
    test_connectivity
    generate_test_data

    echo -e "\n${BLUE}=== Starting Benchmarks ===${NC}"

    run_redis_benchmark
    run_valkey_benchmark
    run_memtier_benchmark
    run_redis_cli_benchmark

    # Cleanup
    rm -f test_*.bin

    echo -e "\n${GREEN}=== Benchmark Suite Completed ===${NC}"
    echo "Check the output above for performance metrics."
    echo "For continuous monitoring, consider running individual tools with longer durations."
}

# Handle command line arguments
case "${1:-}" in
    "redis")
        check_server && run_redis_benchmark
        ;;
    "valkey")
        check_server && run_valkey_benchmark
        ;;

    "memtier")
        check_server && run_memtier_benchmark
        ;;
    "cli")
        check_server && generate_test_data && run_redis_cli_benchmark && rm -f test_*.bin
        ;;
    "help"|"-h"|"--help")
        echo "Usage: $0 [redis|valkey|memtier|cli|help]"
        echo "  redis   - Run redis-benchmark only"
        echo "  valkey  - Run valkey-benchmark only"
        echo "  memtier - Run memtier_benchmark only"
        echo "  cli     - Run redis-cli custom tests only"
        echo "  help    - Show this help message"
        echo "  (no args) - Run all available benchmarks"
        ;;
    "")
        main
        ;;
    *)
        log_error "Unknown argument: $1"
        echo "Use '$0 help' for usage information"
        exit 1
        ;;
esac
