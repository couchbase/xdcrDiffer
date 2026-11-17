module github.com/couchbase/xdcrDiffer

go 1.26.0

replace github.com/couchbase/eventing-ee => ./stubs/eventing-ee

replace github.com/couchbase/query => ./stubs/query

replace github.com/couchbase/regulator => ./stubs/regulator

require (
	github.com/couchbase/gocb/v2 v2.12.5
	github.com/couchbase/gocbcore/v10 v10.9.4
	github.com/couchbase/gomemcached v0.3.4
	github.com/couchbase/goxdcr/v8 v8.1.0-1599
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/stretchr/testify v1.12.0
	golang.org/x/crypto v0.57.0
	golang.org/x/term v0.46.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/alecthomas/participle v0.7.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/corpix/uarand v0.0.0-20170723150923-031be390f409 // indirect
	github.com/couchbase/cbauth v0.1.13 // indirect
	github.com/couchbase/clog v0.1.0 // indirect
	github.com/couchbase/eventing-ee v0.0.0-00010101000000-000000000000 // indirect
	github.com/couchbase/go-couchbase v0.1.1 // indirect
	github.com/couchbase/gocbcore/v9 v9.1.11 // indirect
	github.com/couchbase/gocbcoreps v0.1.5-0.20260107140814-1c3a03f888f8 // indirect
	github.com/couchbase/goprotostellar v1.0.6-0.20260407143512-d7af25156dcc // indirect
	github.com/couchbase/goutils v0.1.2 // indirect
	github.com/couchbase/tools-common/errors v1.0.0 // indirect
	github.com/couchbase/tools-common/http v1.0.7 // indirect
	github.com/couchbaselabs/gocbconnstr/v2 v2.0.0 // indirect
	github.com/couchbaselabs/gojsonsm v1.0.1 // indirect
	github.com/glenn-brown/golang-pkg-pcre v0.0.0-20120522223659-48bb82a8b8ce // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/icrowley/fake v0.0.0-20240710202011-f797eb4a99c0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/stretchr/objx v0.5.3 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.65.0 // indirect
	go.opentelemetry.io/otel v1.40.0 // indirect
	go.opentelemetry.io/otel/metric v1.40.0 // indirect
	go.opentelemetry.io/otel/trace v1.40.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260203192932-546029d2fa20 // indirect
	google.golang.org/grpc v1.79.3 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
