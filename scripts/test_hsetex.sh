#!/bin/bash

# Test script for HSETEX command (Redis 8.0 syntax)
# This script tests the HSETEX implementation in Blobasaur

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_CLI="redis-cli -p $REDIS_PORT"

# Helper functions
print_test() {
    echo -e "${YELLOW}[TEST]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[PASS]${NC} $1"
}

print_error() {
    echo -e "${RED}[FAIL]${NC} $1"
    exit 1
}

assert_equals() {
    local actual="$1"
    local expected="$2"
    local test_name="$3"
    
    if [ "$actual" = "$expected" ]; then
        print_success "$test_name: Got expected value '$expected'"
    else
        print_error "$test_name: Expected '$expected', but got '$actual'"
    fi
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    local test_name="$3"
    
    if [[ "$haystack" == *"$needle"* ]]; then
        print_success "$test_name: Output contains '$needle'"
    else
        print_error "$test_name: Output doesn't contain '$needle'. Got: '$haystack'"
    fi
}

# Start tests
echo "========================================="
echo "Testing HSETEX command (Redis 8.0 syntax)"
echo "========================================="
echo ""

# Test 1: Basic HSETEX with EX option
print_test "Test 1: Setting single field with 3 second TTL using EX"
result=$($REDIS_CLI HSETEX test_sessions EX 3 FIELDS 1 session1 "data1")
assert_equals "$result" "1" "Single field HSETEX"

print_test "Verifying field exists immediately after setting"
result=$($REDIS_CLI HGET test_sessions session1)
assert_equals "$result" "data1" "HGET after HSETEX"

result=$($REDIS_CLI HEXISTS test_sessions session1)
assert_equals "$result" "1" "HEXISTS after HSETEX"

print_test "Waiting for expiration (4 seconds)..."
sleep 4

result=$($REDIS_CLI HGET test_sessions session1)
assert_equals "$result" "" "HGET after expiration"

result=$($REDIS_CLI HEXISTS test_sessions session1)
assert_equals "$result" "0" "HEXISTS after expiration"

# Test 2: Multiple fields with expiration
print_test "Test 2: Setting multiple fields with expiration"
result=$($REDIS_CLI HSETEX test_users EX 10 FIELDS 3 alice "Alice Data" bob "Bob Data" charlie "Charlie Data")
assert_equals "$result" "1" "Multiple fields HSETEX"

result=$($REDIS_CLI HGET test_users alice)
assert_equals "$result" "Alice Data" "HGET alice"

result=$($REDIS_CLI HGET test_users bob)
assert_equals "$result" "Bob Data" "HGET bob"

result=$($REDIS_CLI HGET test_users charlie)
assert_equals "$result" "Charlie Data" "HGET charlie"

# Test 3: PX option (milliseconds)
print_test "Test 3: Setting field with 2000ms (2 second) TTL using PX"
result=$($REDIS_CLI HSETEX test_temp PX 2000 FIELDS 1 tempdata "temporary")
assert_equals "$result" "1" "HSETEX with PX option"

result=$($REDIS_CLI HGET test_temp tempdata)
assert_equals "$result" "temporary" "HGET immediately after PX"

sleep 1
print_test "After 1 second, field should still exist"
result=$($REDIS_CLI HGET test_temp tempdata)
assert_equals "$result" "temporary" "HGET after 1 second"

sleep 1.5
print_test "After 2.5 seconds total, field should be expired"
result=$($REDIS_CLI HGET test_temp tempdata)
assert_equals "$result" "" "HGET after expiration"

# Test 4: FNX option (only set if field doesn't exist)
print_test "Test 4: Testing FNX option"

# First set a field
$REDIS_CLI HSET test_fnx field1 "original" > /dev/null

# Try to overwrite with FNX (should fail)
result=$($REDIS_CLI HSETEX test_fnx FNX EX 10 FIELDS 1 field1 "new_value")
assert_equals "$result" "0" "HSETEX FNX on existing field returns 0"

# Verify original value is unchanged
result=$($REDIS_CLI HGET test_fnx field1)
assert_equals "$result" "original" "Field value unchanged after FNX"

# Try to set a new field with FNX (should succeed)
result=$($REDIS_CLI HSETEX test_fnx FNX EX 10 FIELDS 1 field2 "new_field")
assert_equals "$result" "1" "HSETEX FNX on new field returns 1"

result=$($REDIS_CLI HGET test_fnx field2)
assert_equals "$result" "new_field" "New field set with FNX"

# Test 5: FXX option (only set if field exists)
print_test "Test 5: Testing FXX option"

# Try to set a non-existent field with FXX (should fail)
result=$($REDIS_CLI HSETEX test_fxx FXX EX 10 FIELDS 1 nonexistent "value")
assert_equals "$result" "0" "HSETEX FXX on non-existent field returns 0"

# Set a field first
$REDIS_CLI HSET test_fxx existing "initial" > /dev/null

# Update with FXX (should succeed)
result=$($REDIS_CLI HSETEX test_fxx FXX EX 10 FIELDS 1 existing "updated")
assert_equals "$result" "1" "HSETEX FXX on existing field returns 1"

result=$($REDIS_CLI HGET test_fxx existing)
assert_equals "$result" "updated" "Field updated with FXX"

# Test 6: EXAT option (absolute timestamp)
print_test "Test 6: Testing EXAT option (absolute timestamp)"
# Set expiration to 3 seconds from now
expire_time=$(($(date +%s) + 3))
result=$($REDIS_CLI HSETEX test_exat EXAT $expire_time FIELDS 1 field1 "exat_data")
assert_equals "$result" "1" "HSETEX with EXAT"

result=$($REDIS_CLI HGET test_exat field1)
assert_equals "$result" "exat_data" "HGET after EXAT"

print_test "Waiting 4 seconds for EXAT expiration..."
sleep 4

result=$($REDIS_CLI HGET test_exat field1)
assert_equals "$result" "" "HGET after EXAT expiration"

# Test 7: Mixed field conditions
print_test "Test 7: Testing multiple fields with mixed results"

# Setup: Create one existing field
$REDIS_CLI HSET test_mixed existing_field "exists" > /dev/null

# Try to set both existing and new fields with FNX
result=$($REDIS_CLI HSETEX test_mixed FNX EX 10 FIELDS 2 existing_field "should_fail" new_field "should_succeed")
# In our implementation, if any field fails the condition, the whole operation returns 0
assert_equals "$result" "0" "HSETEX FNX with mixed fields"

# Verify existing field is unchanged
result=$($REDIS_CLI HGET test_mixed existing_field)
assert_equals "$result" "exists" "Existing field unchanged"

# The new field might or might not be set depending on implementation
# (Redis 8.0 would set fields that pass the condition)

# Test 8: No expiration option (fields should persist)
print_test "Test 8: HSETEX without expiration option"
result=$($REDIS_CLI HSETEX test_persist FIELDS 1 permanent "forever")
assert_equals "$result" "1" "HSETEX without TTL"

result=$($REDIS_CLI HGET test_persist permanent)
assert_equals "$result" "forever" "Field set without expiration"

sleep 2
result=$($REDIS_CLI HGET test_persist permanent)
assert_equals "$result" "forever" "Field still exists after time"

# Cleanup
print_test "Cleaning up test data..."
$REDIS_CLI DEL test_sessions test_users test_temp test_fnx test_fxx test_exat test_mixed test_persist > /dev/null

echo ""
echo "========================================="
echo -e "${GREEN}All HSETEX tests passed successfully!${NC}"
echo "========================================="