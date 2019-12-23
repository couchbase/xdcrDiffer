#!/bin/bash

# build Copyright (c) 2013-2019 Couchbase, Inc.
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions
# and limitations under the License.

# Gojsonsm is a library that lives under couchbaselabs
# However, official couchbase builds will clone it under couchbase, and goxdcr would reference
# it there. This is a small workaround by finding the symlink and then linking it accordingly
# so go get goxdcr/base would work
gojsonsmBase="src/github.com/couchbaselabs/gojsonsm"
gojsonsmSymlink="src/github.com/couchbase/gojsonsm"
for path in `echo "$GOPATH" | tr ':' ' '`
do
	if [[ -d "$path/$gojsonsmBase" ]];then
		if [[ -s "$path/$gojsonsmSymlink" ]];then
			# Is already a symlink
			continue
		elif [[ ! -d "$path/$gojsonsmSymlink" ]] && [[ ! -f "$path/$gojsonsmSymlink" ]];then
		echo  "Creating symlink of $gopath/$gojsonsmSymlink linking to $path/$gojsonsmBase"
			mkdir -p `dirname $path/$gojsonsmSymlink`
			ln -s "$path/$gojsonsmBase/" "$path/$gojsonsmSymlink"
		fi
	fi
done

