#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Blobnom Cluster Test Script ===${NC}"

# Function to test a node
test_node() {
    local port=$1
    local node_name=$2

    echo -e "\n${YELLOW}Testing Node $node_name (port $port)...${NC}"

    # Test basic connectivity
    if timeout 5 redis-cli -p $port ping > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Node $node_name is responding${NC}"
    else
        echo -e "${RED}✗ Node $node_name is not responding${NC}"
        return 1
    fi

    # Test cluster info
    echo -e "${BLUE}--- Cluster Info for Node $node_name ---${NC}"
    redis-cli -p $port CLUSTER INFO

    # Test cluster nodes
    echo -e "${BLUE}--- Cluster Nodes for Node $node_name ---${NC}"
    redis-cli -p $port CLUSTER NODES

    echo -e "${BLUE}--- Cluster Slots for Node $node_name ---${NC}"
    redis-cli -p $port CLUSTER SLOTS

    return 0
}

# Function to test key operations
test_key_operations() {
    echo -e "\n${YELLOW}Testing Key Operations...${NC}"

    # Test setting and getting keys that should map to different slots
    local test_keys=("user:1000" "user:2000" "user:3000" "product:abc" "session:xyz")

    for key in "${test_keys[@]}"; do
        # Calculate which slot this key maps to
        slot=$(redis-cli -p 6381 CLUSTER KEYSLOT "$key")
        echo -e "${BLUE}Key '$key' maps to slot $slot${NC}"

        # Try to set the key
        result=$(redis-cli -c -p 6381 SET "$key" "test-value-$key" 2>&1)
        if [[ $result == "OK" ]]; then
            echo -e "${GREEN}✓ SET $key successful${NC}"

            # Try to get the key
            value=$(redis-cli -c -p 6381 GET "$key" 2>&1)
            if [[ $value == "test-value-$key" ]]; then
                echo -e "${GREEN}✓ GET $key successful: $value${NC}"
            else
                echo -e "${RED}✗ GET $key failed: $value${NC}"
            fi
        else
            echo -e "${YELLOW}→ SET $key result: $result${NC}"
        fi
    done
}

# Function to check cluster health
check_cluster_health() {
    echo -e "\n${YELLOW}Checking Cluster Health...${NC}"

    local nodes_info=$(redis-cli -p 6381 CLUSTER NODES 2>/dev/null)
    local node_count=$(echo "$nodes_info" | wc -l)
    local master_count=$(echo "$nodes_info" | grep -c "master")

    echo -e "${BLUE}Total nodes detected: $node_count${NC}"
    echo -e "${BLUE}Master nodes: $master_count${NC}"

    # Check if all slots are covered
    local cluster_info=$(redis-cli -p 6381 CLUSTER INFO 2>/dev/null)
    local slots_assigned=$(echo "$cluster_info" | grep "cluster_slots_assigned" | cut -d: -f2)
    local cluster_state=$(echo "$cluster_info" | grep "cluster_state" | cut -d: -f2)

    echo -e "${BLUE}Cluster state: $cluster_state${NC}"
    echo -e "${BLUE}Slots assigned: $slots_assigned/16384${NC}"

    if [[ $cluster_state == "ok" ]]; then
        echo -e "${GREEN}✓ Cluster is healthy${NC}"
        return 0
    else
        echo -e "${RED}✗ Cluster is not healthy${NC}"
        return 1
    fi
}

# Main test execution
echo -e "${BLUE}Waiting for nodes to start up...${NC}"
sleep 5

# Test individual nodes
test_node 6381 "1"
test_node 6382 "2"
test_node 6383 "3"

# Check overall cluster health
check_cluster_health

# Test key operations
test_key_operations

echo -e "\n${BLUE}=== Test Complete ===${NC}"

# Show logs for debugging if cluster is not healthy
if ! check_cluster_health > /dev/null 2>&1; then
    echo -e "\n${YELLOW}Cluster appears unhealthy. Showing recent logs...${NC}"
    echo -e "${BLUE}--- Node 1 logs ---${NC}"
    docker logs --tail 20 blobnom-node1 2>/dev/null || echo "Could not get node1 logs"
    echo -e "${BLUE}--- Node 2 logs ---${NC}"
    docker logs --tail 20 blobnom-node2 2>/dev/null || echo "Could not get node2 logs"
    echo -e "${BLUE}--- Node 3 logs ---${NC}"
    docker logs --tail 20 blobnom-node3 2>/dev/null || echo "Could not get node3 logs"
fi
