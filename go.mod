module github.com/couchbase/xdcrDiffer

go 1.23.0

toolchain go1.23.5

replace github.com/couchbase/eventing-ee => ./stubs/eventing-ee

replace github.com/couchbase/query => ./stubs/query

replace github.com/couchbase/regulator => ./stubs/regulator

require (
	github.com/couchbase/gocb/v2 v2.9.4
	github.com/couchbase/gocbcore/v10 v10.5.4
	github.com/couchbase/gomemcached v0.3.3
	github.com/couchbase/goxdcr/v8 v8.0.0-3523
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/stretchr/testify v1.10.0
	golang.org/x/term v0.30.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/alecthomas/participle v0.7.1 // indirect
	github.com/corpix/uarand v0.0.0-20170723150923-031be390f409 // indirect
	github.com/couchbase/cbauth v0.1.13 // indirect
	github.com/couchbase/clog v0.1.0 // indirect
	github.com/couchbase/eventing-ee v0.0.0-00010101000000-000000000000 // indirect
	github.com/couchbase/go-couchbase v0.1.1 // indirect
	github.com/couchbase/gocbcore/v9 v9.1.11 // indirect
	github.com/couchbase/gocbcoreps v0.1.3 // indirect
	github.com/couchbase/goprotostellar v1.0.2 // indirect
	github.com/couchbase/goutils v0.1.2 // indirect
	github.com/couchbase/tools-common/errors v1.0.0 // indirect
	github.com/couchbase/tools-common/http v1.0.7 // indirect
	github.com/couchbaselabs/gocbconnstr/v2 v2.0.0-20240607131231-fb385523de28 // indirect
	github.com/couchbaselabs/gojsonsm v1.0.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/glenn-brown/golang-pkg-pcre v0.0.0-20120522223659-48bb82a8b8ce // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
	github.com/icrowley/fake v0.0.0-20240710202011-f797eb4a99c0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0 // indirect
	go.opentelemetry.io/otel v1.24.0 // indirect
	go.opentelemetry.io/otel/metric v1.24.0 // indirect
	go.opentelemetry.io/otel/trace v1.24.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/crypto v0.36.0 // indirect
	golang.org/x/net v0.25.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240401170217-c3f982113cda // indirect
	google.golang.org/grpc v1.63.2 // indirect
	google.golang.org/protobuf v1.36.5 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
)
