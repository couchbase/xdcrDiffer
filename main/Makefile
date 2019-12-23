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
BINARY_NAME=xdcrDiffer

all: build
build: 
	$(GOBUILD) -o $(BINARY_NAME) -v
clean: 
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
deps:
	$(GOGET) github.com/couchbaselabs/gojsonsm
	./makefileHelper.sh
	$(GOGET) github.com/couchbase/goxdcr/base
	$(GOGET) github.com/couchbase/goxdcr/log
	$(GOGET) github.com/couchbase/goxdcr/metadata
	$(GOGET) github.com/couchbase/goxdcr/metadata_svc
	$(GOGET) github.com/couchbase/goxdcr/parts
	$(GOGET) github.com/couchbase/goxdcr/service_def
	$(GOGET) github.com/couchbase/goxdcr/service_def/mocks
	$(GOGET) github.com/couchbase/goxdcr/utils
	$(GOGET) gopkg.in/couchbase/gocb.v1
	$(GOGET) gopkg.in/couchbase/gocbcore.v7
	$(GOGET) github.com/couchbase/gomemcached
	$(GOGET) github.com/rcrowley/go-metrics
