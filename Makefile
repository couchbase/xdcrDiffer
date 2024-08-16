# build Copyright (c) 2013-2020 Couchbase, Inc.
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions
# and limitations under the License.

# Go parameters
GOCMD=go
GOMOD_FILE=go-public.mod
GOMOD_SUM=go-public.sum
GOGET=$(GOCMD) get -modfile $(GOMOD_FILE)
GOBUILD=$(GOCMD) build -modfile $(GOMOD_FILE)
BINARY_NAME=xdcrDiffer

all: build
build:
	$(GOBUILD) -o $(BINARY_NAME) -v
deps:
	$(GOGET) github.com/couchbase/gocbcore/v10
	$(GOGET) github.com/couchbase/gocb/v2
	$(GOGET) github.com/couchbase/goxdcr@v8.0.0-1706
	$(GOGET) github.com/rcrowley/go-metrics
	$(GOGET) github.com/couchbase/cbauth@v0.1.5
	$(GOGET) github.com/couchbase/gomemcached@v0.3.1
	$(GOGET) github.com/couchbase/go-couchbase@v0.1.0
	$(GOGET) github.com/couchbase/goutils@v0.1.0
	$(GOGET) golang.org/x/crypto
	$(GOGET) golang.org/x/net
	$(GOGET) github.com/couchbase/clog
	$(GOGET) github.com/stretchr/testify/assert
	$(GOGET) github.com/stretchr/testify/mock
	$(GOGET) github.com/couchbaselabs/gojsonsm@v1.0.1
	$(GOGET) google.golang.org/grpc/internal@v1.63.2
	$(GOGET) golang.org/x/net/idna@v0.28.0

clean:
	rm $(BINARY_NAME) $(GOMOD_FILE) $(GOSUM_FILE)
