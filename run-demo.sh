#!/bin/bash
# Tadka Kafka Demo Runner — Linux/Mac
# Usage: chmod +x run-demo.sh && ./run-demo.sh

set -e

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
WHITE='\033[1;37m'
GRAY='\033[0;37m'
NC='\033[0m'

print_header() {
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  🍛 Tadka Kafka Demo — The Desi Architect${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════${NC}"
    echo ""
}

start_kafka() {
    echo -e "${YELLOW}  Starting Kafka & Kafka UI...${NC}"
    docker compose up -d
    echo -e "${YELLOW}  Waiting for Kafka to be ready...${NC}"
    
    retries=0
    max_retries=30
    while [ $retries -lt $max_retries ]; do
        health=$(docker inspect --format='{{.State.Health.Status}}' demo-kafka-1 2>/dev/null || echo "not found")
        if [ "$health" = "healthy" ]; then
            echo -e "${GREEN}  ✅ Kafka is ready!${NC}"
            return
        fi
        sleep 2
        retries=$((retries + 1))
        echo -e "${GRAY}  ⏳ Waiting... ($retries/$max_retries)${NC}"
    done
    echo -e "${YELLOW}  ⚠️  Kafka may not be fully ready. Proceeding anyway.${NC}"
}

show_menu() {
    echo ""
    echo -e "${WHITE}  Pick a demo:${NC}"
    echo ""
    echo -e "  1) Setup — Create topics"
    echo -e "  2) Producer — Send orders"
    echo -e "  3) Consumers — Run all 4 consumers"
    echo -e "  4) Partition Demo — Key routing & ordering"
    echo -e "  5) Scaling Demo — Consumer golden rule"
    echo -e "  6) Hot Partition — Skew + compound key fix"
    echo -e "  7) Delivery Guarantees — At-most/least/idempotent"
    echo -e "  8) Offset Reset — Replay from beginning"
    echo -e "  9) Open Kafka UI"
    echo -e "  0) Stop & cleanup"
    echo ""
}

# Main
print_header

# Check prerequisites
if ! command -v docker &> /dev/null; then
    echo -e "${RED}  ❌ Docker not found. Install Docker first.${NC}"
    exit 1
fi
if ! command -v node &> /dev/null; then
    echo -e "${RED}  ❌ Node.js not found. Install Node.js 18+ first.${NC}"
    exit 1
fi

# Install dependencies if needed
if [ ! -d "node_modules" ]; then
    echo -e "${YELLOW}  Installing dependencies...${NC}"
    npm install
fi

# Start Kafka
start_kafka

while true; do
    show_menu
    read -p "  Enter choice: " choice
    
    case $choice in
        1) npm run setup ;;
        2) npm run producer ;;
        3)
            echo -e "${YELLOW}  Starting all 4 consumers in background...${NC}"
            npm run notification &
            npm run analytics &
            npm run restaurant &
            npm run search-indexer &
            echo -e "${GREEN}  ✅ 4 consumers started in background. Press Enter to continue.${NC}"
            read
            ;;
        4) npm run partition-demo ;;
        5)
            echo -e "${YELLOW}  Starting 3 consumer instances...${NC}"
            node scaling-demo.js 1 &
            node scaling-demo.js 2 &
            node scaling-demo.js 3 &
            echo -e "${GREEN}  ✅ 3 instances started. Try: node scaling-demo.js 4${NC}"
            read
            ;;
        6) npm run hot-partition ;;
        7)
            echo ""
            echo "  a) At-Most-Once"
            echo "  b) At-Least-Once"
            echo "  c) Idempotent Consumer"
            echo ""
            read -p "  Pick (a/b/c): " sub
            case $sub in
                a) npm run at-most-once ;;
                b) npm run at-least-once ;;
                c) npm run idempotent ;;
                *) echo -e "${RED}  Invalid choice${NC}" ;;
            esac
            ;;
        8) npm run offset-reset ;;
        9)
            if command -v xdg-open &> /dev/null; then
                xdg-open "http://localhost:8080"
            elif command -v open &> /dev/null; then
                open "http://localhost:8080"
            else
                echo "  Open http://localhost:8080 in your browser"
            fi
            ;;
        0)
            echo -e "${YELLOW}  Stopping Kafka...${NC}"
            docker compose down -v
            echo -e "${GREEN}  ✅ Cleaned up!${NC}"
            exit 0
            ;;
        *) echo -e "${RED}  Invalid choice${NC}" ;;
    esac
done
