module github.com/the-protobuf-project/runtime-go/network

go 1.26.0

require (
	github.com/gorilla/websocket v1.5.3
	github.com/hasura/go-graphql-client v0.16.0
	github.com/joho/godotenv v1.5.1
	github.com/stretchr/testify v1.12.1
)

require (
	github.com/coder/websocket v1.8.15 // indirect
	github.com/google/uuid v1.6.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
)

require github.com/the-protobuf-project/runtime-go/telemetry v0.0.0-20261021141431-6d4588022f2b

replace github.com/the-protobuf-project/runtime-go/telemetry => ../telemetry
