#!/bin/bash

echo "Starting gqlgen GraphQL Server..."
echo ""

cd "$(dirname "$0")/server"

# Download dependencies
go mod tidy

# Run server
go run .