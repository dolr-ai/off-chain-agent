#!/bin/bash

# Script to run the QStash automated tournament test with proper timestamps
# Usage: ./run_tournament_test.sh [duration_in_seconds]

# Default tournament duration (5 minutes for testing, can be overridden)
DURATION=${1:-300}

# Check required environment variables
if [ -z "$QSTASH_AUTH_TOKEN" ]; then
    echo "Error: QSTASH_AUTH_TOKEN environment variable is not set"
    echo "Please set: export QSTASH_AUTH_TOKEN=your_token"
    exit 1
fi

if [ -z "$GRPC_AUTH_TOKEN" ]; then
    echo "Error: GRPC_AUTH_TOKEN environment variable is not set"
    echo "Please set: export GRPC_AUTH_TOKEN=your_token"
    exit 1
fi

# Calculate timestamps
START_TIME=$(date +%s)
END_TIME=$((START_TIME + DURATION))

echo "========================================="
echo "QStash Automated Tournament Test"
echo "========================================="
echo "Start Time: $(date -r $START_TIME '+%Y-%m-%d %H:%M:%S')"
echo "End Time:   $(date -r $END_TIME '+%Y-%m-%d %H:%M:%S')"
echo "Duration:   $DURATION seconds"
echo "========================================="
echo ""

# Run the hurl test
echo "Starting tournament test..."
hurl \
    --variable qstash_token="$QSTASH_AUTH_TOKEN" \
    --variable auth_token="$GRPC_AUTH_TOKEN" \
    --variable start_time="$START_TIME" \
    --variable end_time="$END_TIME" \
    test_leaderboard_qstash_auto.hurl

if [ $? -eq 0 ]; then
    echo ""
    echo "========================================="
    echo "Tournament created and scores updated successfully!"
    echo "The tournament will automatically finalize at:"
    echo "$(date -r $END_TIME '+%Y-%m-%d %H:%M:%S')"
    echo ""
    echo "To check tournament status:"
    echo "curl http://localhost:50051/api/v1/leaderboard/current"
    echo ""
    echo "After finalization, check results with:"
    echo "curl http://localhost:50051/api/v1/leaderboard/history?limit=1"
    echo "========================================="
else
    echo ""
    echo "Test failed! Please check the error messages above."
    exit 1
fi