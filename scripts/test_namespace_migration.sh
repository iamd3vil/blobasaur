#!/bin/bash

# Test script for namespace shard migration in Blobasaur
# This script tests that HSET/HGET namespaced data is correctly migrated between different shard configurations

set -e

# Configuration
SERVER_ADDR="127.0.0.1:6380"
DATA_DIR="./data"
VALKEY_CLI="valkey-cli -p 6380"

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

# Function to check if server is running
check_server() {
    if ! $VALKEY_CLI ping >/dev/null 2>&1; then
        log_error "Blobasaur server is not running on $SERVER_ADDR"
        log_info "Please start the server with: cargo run --release"
        exit 1
    fi
    log_success "Server is running"
}

# Function to populate test data
populate_test_data() {
    log_info "Populating test data with namespaced entries..."

    # Create data in multiple namespaces
    local namespaces=("users" "products" "sessions" "cache" "analytics")
    local keys=("profile" "settings" "metadata" "data1" "data2" "data3")

    local total_entries=0

    for namespace in "${namespaces[@]}"; do
        for key in "${keys[@]}"; do
            for i in {1..5}; do
                local full_key="${key}_${i}"
                local value="namespace:${namespace}|key:${full_key}|data:$(date +%s)_${RANDOM}"

                $VALKEY_CLI HSET "$namespace" "$full_key" "$value" >/dev/null
                total_entries=$((total_entries + 1))
            done
        done
    done

    # Add some regular (non-namespaced) keys for comparison
    for i in {1..10}; do
        local key="regular_key_${i}"
        local value="regular_value_${i}_$(date +%s)"
        $VALKEY_CLI SET "$key" "$value" >/dev/null
        total_entries=$((total_entries + 1))
    done

    log_success "Created $total_entries total entries"
    return $total_entries
}

# Function to verify data integrity
verify_data_integrity() {
    local expected_count=$1
    log_info "Verifying data integrity..."

    local namespaces=("users" "products" "sessions" "cache" "analytics")
    local keys=("profile" "settings" "metadata" "data1" "data2" "data3")

    local found_entries=0
    local missing_entries=0

    # Verify namespaced data
    for namespace in "${namespaces[@]}"; do
        for key in "${keys[@]}"; do
            for i in {1..5}; do
                local full_key="${key}_${i}"
                local expected_prefix="namespace:${namespace}|key:${full_key}|data:"

                local value=$($VALKEY_CLI HGET "$namespace" "$full_key" 2>/dev/null || echo "")
                if [[ -n "$value" && "$value" == $expected_prefix* ]]; then
                    found_entries=$((found_entries + 1))
                else
                    log_error "Missing or corrupted: namespace=$namespace, key=$full_key"
                    missing_entries=$((missing_entries + 1))
                fi
            done
        done
    done

    # Verify regular keys
    for i in {1..10}; do
        local key="regular_key_${i}"
        local expected_prefix="regular_value_${i}_"

        local value=$($VALKEY_CLI GET "$key" 2>/dev/null || echo "")
        if [[ -n "$value" && "$value" == $expected_prefix* ]]; then
            found_entries=$((found_entries + 1))
        else
            log_error "Missing or corrupted regular key: $key"
            missing_entries=$((missing_entries + 1))
        fi
    done

    log_info "Found: $found_entries entries, Missing: $missing_entries entries"

    if [[ $missing_entries -eq 0 && $found_entries -eq $expected_count ]]; then
        log_success "All data verified successfully!"
        return 0
    else
        log_error "Data integrity check failed!"
        return 1
    fi
}

# Function to display current shard info
show_shard_info() {
    log_info "Current data directory contents:"
    if [[ -d "$DATA_DIR" ]]; then
        ls -la "$DATA_DIR"/
        echo
        log_info "Shard database files:"
        for db in "$DATA_DIR"/shard_*.db; do
            if [[ -f "$db" ]]; then
                local size=$(du -h "$db" | cut -f1)
                echo "  $(basename "$db"): $size"
            fi
        done
    else
        log_warning "Data directory $DATA_DIR does not exist"
    fi
}

# Function to backup current data
backup_data() {
    local backup_dir="${DATA_DIR}_backup_$(date +%Y%m%d_%H%M%S)"
    log_info "Creating backup at $backup_dir"

    if [[ -d "$DATA_DIR" ]]; then
        cp -r "$DATA_DIR" "$backup_dir"
        log_success "Backup created: $backup_dir"
        echo "$backup_dir"
    else
        log_warning "No data directory to backup"
        echo ""
    fi
}

# Function to stop server (if running)
stop_server() {
    log_info "Attempting to stop server..."
    pkill -f blobasaur || true
    sleep 2

    # Verify it's stopped
    if $VALKEY_CLI ping >/dev/null 2>&1; then
        log_warning "Server is still running, you may need to stop it manually"
        return 1
    else
        log_success "Server stopped"
        return 0
    fi
}

# Function to validate migration parameters
validate_migration() {
    local old_shards=$1
    local new_shards=$2

    # Check if parameters are numbers
    if ! [[ "$old_shards" =~ ^[0-9]+$ ]] || ! [[ "$new_shards" =~ ^[0-9]+$ ]]; then
        log_error "Shard counts must be positive integers"
        return 1
    fi

    # Check if both are positive
    if [[ $old_shards -le 0 ]] || [[ $new_shards -le 0 ]]; then
        log_error "Shard counts must be greater than 0"
        return 1
    fi

    # Check for shard reduction
    if [[ $new_shards -lt $old_shards ]]; then
        log_error "Cannot reduce number of shards from $old_shards to $new_shards"
        log_error "Shard reduction can lead to data loss and is not supported"
        log_error "This script only supports SCALING UP operations"
        log_info "You can only increase the number of shards for scaling up"
        return 1
    fi

    # Check if no change
    if [[ $new_shards -eq $old_shards ]]; then
        log_warning "No migration needed: both old and new shard counts are $old_shards"
        return 1
    fi

    return 0
}

# Main test function
run_migration_test() {
    local old_shards=$1
    local new_shards=$2

    # Validate migration parameters first
    if ! validate_migration $old_shards $new_shards; then
        return 1
    fi

    log_info "=========================================="
    log_info "Testing migration from $old_shards to $new_shards shards"
    log_info "=========================================="

    # Step 1: Verify server is running and populate data
    check_server
    show_shard_info

    log_info "Populating test data..."
    populate_test_data
    local total_entries=$?

    log_info "Verifying initial data..."
    if ! verify_data_integrity $total_entries; then
        log_error "Initial data verification failed"
        return 1
    fi

    # Step 2: Create backup
    local backup_dir=$(backup_data)

    # Step 3: Stop server for migration
    if ! stop_server; then
        log_error "Could not stop server for migration"
        return 1
    fi

    # Step 4: Run migration
    log_info "Running shard migration: $old_shards -> $new_shards"

    # Try both possible binary locations
    local blobasaur_cmd=""
    if [[ -f "./target/release/blobasaur" ]]; then
        blobasaur_cmd="./target/release/blobasaur"
    elif [[ -f "./target/debug/blobasaur" ]]; then
        blobasaur_cmd="./target/debug/blobasaur"
    else
        log_error "Blobasaur binary not found. Please build with: cargo build --release"
        return 1
    fi

    log_info "Using binary: $blobasaur_cmd"

    if ! $blobasaur_cmd shard migrate $old_shards $new_shards --verify; then
        log_error "Migration failed!"

        # Restore from backup if available
        if [[ -n "$backup_dir" && -d "$backup_dir" ]]; then
            log_info "Restoring from backup..."
            rm -rf "$DATA_DIR"
            cp -r "$backup_dir" "$DATA_DIR"
            log_info "Backup restored"
        fi

        return 1
    fi

    log_success "Migration completed successfully!"

    # Step 5: Update config and restart server
    log_info "Migration completed. Please update config.toml to set num_shards = $new_shards"
    log_info "Then restart the server to test the migrated data"

    read -p "Press Enter after you've updated config.toml and restarted the server..."

    # Step 6: Verify migrated data
    log_info "Waiting for server to start..."
    local retries=10
    while [[ $retries -gt 0 ]]; do
        if $VALKEY_CLI ping >/dev/null 2>&1; then
            break
        fi
        sleep 1
        retries=$((retries - 1))
    done

    if [[ $retries -eq 0 ]]; then
        log_error "Server did not start within expected time"
        return 1
    fi

    log_info "Server is back online, verifying migrated data..."
    show_shard_info

    if verify_data_integrity $total_entries; then
        log_success "=========================================="
        log_success "Migration test PASSED! ✓"
        log_success "All namespaced data migrated correctly"
        log_success "=========================================="

        # Cleanup backup
        if [[ -n "$backup_dir" && -d "$backup_dir" ]]; then
            log_info "Cleaning up backup directory: $backup_dir"
            rm -rf "$backup_dir"
        fi

        return 0
    else
        log_error "=========================================="
        log_error "Migration test FAILED! ✗"
        log_error "Data verification failed after migration"
        log_error "=========================================="

        # Keep backup for investigation
        if [[ -n "$backup_dir" ]]; then
            log_info "Backup preserved for investigation: $backup_dir"
        fi

        return 1
    fi
}

# Function to run multiple test scenarios
run_all_tests() {
    local test_scenarios=(
        "2 4"  # Double the shards
        "4 6"  # Increase by 2
        "6 8"  # Increase by 2 more
        "8 12" # Increase by 4
    )

    log_info "Running comprehensive namespace migration tests..."
    log_info "Test scenarios: ${#test_scenarios[@]} different shard migrations (SCALING UP ONLY)"

    local passed=0
    local failed=0

    for scenario in "${test_scenarios[@]}"; do
        local old_shards=$(echo $scenario | cut -d' ' -f1)
        local new_shards=$(echo $scenario | cut -d' ' -f2)

        echo
        log_info "=================== TEST SCENARIO ==================="
        log_info "Migration: $old_shards shards → $new_shards shards (SCALING UP)"
        log_info "=================================================="

        if run_migration_test $old_shards $new_shards; then
            passed=$((passed + 1))
            log_success "Scenario $old_shards→$new_shards: PASSED"
        else
            failed=$((failed + 1))
            log_error "Scenario $old_shards→$new_shards: FAILED"

            read -p "Continue with remaining tests? (y/N): " continue_tests
            if [[ "$continue_tests" != "y" && "$continue_tests" != "Y" ]]; then
                log_info "Stopping tests as requested..."
                break
            fi
        fi

        echo
        log_info "Test progress: $passed passed, $failed failed"
        sleep 2
    done

    echo
    log_info "=========================================="
    log_info "FINAL TEST RESULTS"
    log_info "=========================================="
    log_info "Total scenarios: ${#test_scenarios[@]} (SCALING UP ONLY)"
    log_success "Passed: $passed"
    if [[ $failed -gt 0 ]]; then
        log_error "Failed: $failed"
    else
        log_info "Failed: $failed"
    fi

    if [[ $failed -eq 0 ]]; then
        log_success "🎉 ALL NAMESPACE MIGRATION TESTS PASSED! 🎉"
    else
        log_error "❌ Some tests failed. Please investigate."
    fi

    log_info "All tests completed. Script will now exit."
}

# Main script execution
main() {
    echo "============================================"
    echo "Blobasaur Namespace Migration Test Script"
    echo "============================================"
    echo
    log_warning "IMPORTANT: This script only supports SCALING UP (INCREASING shard count)"
    log_warning "Reducing shards (e.g., 12→7) can lead to data loss and is NOT supported"
    log_warning "All test scenarios will only test scaling UP operations"
    echo

    # Check dependencies
    if ! command -v valkey-cli &>/dev/null; then
        log_error "valkey-cli not found. Please install valkey or redis-cli"
        exit 1
    fi

    # Parse command line arguments
    case "${1:-}" in
    "single")
        local old_shards=${2:-2}
        local new_shards=${3:-4}
        run_migration_test $old_shards $new_shards
        ;;
    "all")
        run_all_tests
        ;;
    *)
        echo "Usage: $0 [single|all] [old_shards] [new_shards]"
        echo
        echo "IMPORTANT: ONLY SCALING UP IS SUPPORTED"
        echo "  - new_shards MUST be greater than old_shards"
        echo "  - Reducing shards (e.g., 12→7) is NOT supported"
        echo
        echo "Examples:"
        echo "  $0 single 2 4      # Test migration from 2 to 4 shards (✓ valid)"
        echo "  $0 single 4 2      # INVALID - cannot reduce shards (✗)"
        echo "  $0 all              # Run all test scenarios (only scaling up)"
        echo "  $0                  # Interactive mode (default)"
        echo

        # Interactive mode
        echo "Select test mode:"
        echo "1) Single migration test"
        echo "2) All migration scenarios"
        echo "3) Exit"
        read -p "Choice (1-3): " choice

        case $choice in
        1)
            read -p "Enter current number of shards: " old_shards
            read -p "Enter target number of shards: " new_shards
            run_migration_test $old_shards $new_shards
            ;;
        2)
            run_all_tests
            ;;
        3)
            log_info "Exiting..."
            exit 0
            ;;
        *)
            log_error "Invalid choice"
            exit 1
            ;;
        esac
        ;;
    esac
}

# Run the script
main "$@"
