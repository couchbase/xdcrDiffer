module xdcrDiffer

go 1.22.5

replace github.com/couchbase/eventing-ee => ../eventing-ee

replace github.com/couchbase/gomemcached => ../gomemcached

replace github.com/couchbase/regulator => ../regulator

replace github.com/couchbase/go_json => ../go_json

replace github.com/couchbase/query => ../query

replace github.com/couchbase/gocb/v2 => ../../../../../godeps/src/github.com/couchbase/gocb/v2

require (
    github.com/couchbase/gocb/v2 v2.9.1
    github.com/couchbase/gocbcore/v10 v10.5.1
    github.com/couchbase/gomemcached v0.3.1
    github.com/couchbase/goxdcr v8.0.0-1706+incompatible
    github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
)

require (
    github.com/alecthomas/participle v0.7.1 // indirect
    github.com/corpix/uarand v0.0.0-20170723150923-031be390f409 // indirect
    github.com/couchbase/cbauth v0.1.12 // indirect
    github.com/couchbase/clog v0.1.0 // indirect
    github.com/couchbase/go-couchbase v0.1.1 // indirect
    github.com/couchbase/gocbcoreps v0.1.3 // indirect
    github.com/couchbase/goprotostellar v1.0.2 // indirect
    github.com/couchbase/goutils v0.1.2 // indirect
    github.com/couchbase/xdcrDiffer v0.0.0-20240718154815-693fa4c30a47 // indirect
    github.com/couchbaselabs/gocbconnstr/v2 v2.0.0-20240607131231-fb385523de28 // indirect
    github.com/couchbaselabs/gojsonsm v1.0.1 // indirect
    github.com/davecgh/go-spew v1.1.1 // indirect
    github.com/glenn-brown/golang-pkg-pcre v0.0.0-20120522223659-48bb82a8b8ce // indirect
    github.com/go-logr/logr v1.4.1 // indirect
    github.com/go-logr/stdr v1.2.2 // indirect
    github.com/golang/snappy v0.0.4 // indirect
    github.com/google/uuid v1.6.0 // indirect
    github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
    github.com/icrowley/fake v0.0.0-20240710202011-f797eb4a99c0 // indirect
    github.com/pkg/errors v0.9.1 // indirect
    github.com/pmezard/go-difflib v1.0.0 // indirect
    github.com/stretchr/objx v0.5.2 // indirect
    github.com/stretchr/testify v1.9.0 // indirect
    go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0 // indirect
    go.opentelemetry.io/otel v1.24.0 // indirect
    go.opentelemetry.io/otel/metric v1.24.0 // indirect
    go.opentelemetry.io/otel/trace v1.24.0 // indirect
    go.uber.org/multierr v1.11.0 // indirect
    go.uber.org/zap v1.27.0 // indirect
    golang.org/x/crypto v0.25.0 // indirect
    golang.org/x/net v0.27.0 // indirect
    golang.org/x/sys v0.22.0 // indirect
    golang.org/x/text v0.16.0 // indirect
    google.golang.org/genproto/googleapis/rpc v0.0.0-20240401170217-c3f982113cda // indirect
    google.golang.org/grpc v1.63.2 // indirect
    google.golang.org/protobuf v1.33.0 // indirect
    gopkg.in/yaml.v3 v3.0.1 // indirect
)
