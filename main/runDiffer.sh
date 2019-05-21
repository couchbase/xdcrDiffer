#!/bin/bash

# Copyright (c) 2013-2019 Couchbase, Inc.
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions
# and limitations under the License.

run_args=$@

execGo="xdcrDiffer"

function printHelp() {
cat << EOF
Usage: $0 -u <username> -p <password> -h <hostname:port> -r <remoteClusterName> -s <sourceBucket> -t <targetBucket> [-n <remoteClusterUsername> -q <remoteClusterPassword>]

This script will set up the necessary environment variable to allow the XDCR diff tool to connect to the metakv service in the
specified source cluster (NOTE: over http://) and retrieve the specified replication spec and run the difftool on it.
The difftool currently only supports connecting to remote targets with username and password. Thus, if the specified remote cluster
reference only contains certificate, then specify the remoteClusterUsername and remoteClusterPassword accordingly.
EOF
}

while getopts ":h:p:u:r:s:t:n:q:" opt; do
  case ${opt} in
    u )
      username=$OPTARG
      ;;
    p )
      password=$OPTARG
      ;;
    h )
      hostname=$OPTARG
      ;;
    r )
      remoteClusterName=$OPTARG
      ;;
    s )
      sourceBucketName=$OPTARG
      ;;
    t )
      targetBucketName=$OPTARG
      ;;
    n )
      remoteClusterUsername=$OPTARG
      ;;
    q )
      remoteClusterPassword=$OPTARG
      ;;
    \? )
      echo "Invalid option: $OPTARG" 1>&2
      ;;
    : )
      echo "Invalid option: $OPTARG requires an argument" 1>&2
      ;;
  esac
done
shift $((OPTIND -1))

if [[ -z "$username" ]];then
	echo "Missing username"
	printHelp
	exit 1
elif [[ -z "$password" ]];then
	echo "Missing password"
	printHelp
	exit 1
elif [[ -z "$hostname" ]];then
	echo "Missing hostname and port"
	printHelp
	exit 1
elif [[ -z "$sourceBucketName" ]];then
	echo "Missing sourceBucket"
	printHelp
	exit 1
elif [[ -z "$targetBucketName" ]];then
	echo "Missing targetBucket"
	printHelp
	exit 1
elif [[ -z "$remoteClusterName" ]];then
	echo "Missing remoteCluster"
	printHelp
	exit 1
fi

go build -o $execGo
if (( $? != 0 ));then
	echo "Unable to build xdcr diff tool"
	exit 1
fi

export CBAUTH_REVRPC_URL="http://$username:$password@$hostname"
echo "Exporting $CBAUTH_REVRPC_URL"

if [[ ! -z "$remoteClusterUsername" ]] && [[ ! -z "$remoteClusterPassword" ]];then
	./$execGo -sourceUrl "$hostname" -sourceUsername $username -sourcePassword $password -sourceBucketName $sourceBucketName -targetBucketName $targetBucketName -remoteClusterName $remoteClusterName -targetUsername $remoteClusterUsername -targetPassword $remoteClusterPassword
else
	./$execGo -sourceUrl "$hostname" -sourceUsername $username -sourcePassword $password -sourceBucketName $sourceBucketName -targetBucketName $targetBucketName -remoteClusterName $remoteClusterName
fi

unset CBAUTH_REVRPC_URL
