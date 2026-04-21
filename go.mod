module github.com/pierre21carrion-hash/raft-consensus

go 1.22

require (
	go.uber.org/zap v1.27.0
	github.com/prometheus/client_golang v1.19.0
	go.opentelemetry.io/otel v1.24.0
	go.opentelemetry.io/otel/trace v1.24.0
	go.opentelemetry.io/otel/sdk v1.24.0
	go.opentelemetry.io/otel/exporters/prometheus v0.46.0
	google.golang.org/grpc v1.62.0
	google.golang.org/protobuf v1.33.0
	github.com/stretchr/testify v1.9.0
)
