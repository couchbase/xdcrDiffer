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
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
BINARY_NAME=xdcrDiffer
GOMOD_FILE=go.mod
GOMOD_SUM=go.sum

all: build
build: 
	$(GOBUILD) -o $(BINARY_NAME) -v
clean: 
	rm $(GOMOD_FILE)
	rm $(GOMOD_SUM)
	rm -f $(BINARY_NAME)
	$(GOCLEAN) -modcache
deps:
	$(GOMOD) init xdcrDiffer
	$(GOGET) github.com/couchbase/gocbcore/v10
	$(GOGET) github.com/couchbase/gocb/v2
	$(GOGET) github.com/couchbaselabs/gojsonsm@v1.0.0
	$(GOGET) github.com/couchbase/goxdcr@v8.0.0-1633
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
