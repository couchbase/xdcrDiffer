#!/bin/bash

#build Copyright (c) 2013-2021 Couchbase, Inc.
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions
# and limitations under the License.

run_args=$@

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
execGo="${SCRIPT_DIR}/xdcrDiffer"

function findExec() {
	if [[ ! -f "$execGo" ]]; then
		echo "Unable to find xdcr diff tool. Did you run make?"
		exit 1
	fi
}

function printHelp() {
	findExec

	cat <<EOF
This script will set up the necessary environment variable to allow the XDCR diff tool to connect to the metakv service in the
specified source cluster (NOTE: over http://) and retrieve the specified replication spec and run the difftool on it.

Usage:
    ${BASH_SOURCE[0]} --username=<username> --password=<password> --hostname=<host:port> --sourceBucket=<sourceBucketName> --targetBucket=<targetBucketName> --remoteClusterName=<remoteClusterRefName> [--clear <To clean before run>] [--compareType=<meta | body | both>] [--xattrExcludeKeysFile=<path/to/file>]

Options:
    -h <host:port> OR --hostname=<host:port>                     : Specify Couchbase server hostname and port number.
    -p <password> OR --password=<password>                       : Specify Couchbase server password.
    -u <username> OR --username=<username>                       : Specify Couchbase server username.
    -r <remoteClusterName> OR --remoteClusterName=<name>         : Specify the remote cluster name.
    -s <sourceBucket> OR --sourceBucket=<bucket>                 : Specify the source bucket.
    -t <targetBucket> OR --targetBucket=<bucket>                 : Specify the target bucket.
    [-m <meta|body|both>] OR [--compareType=<meta|body|both>]    : Specify the type of comparison to perform (meta, body, both). By default it is meta.
    [-e <retryCount>]  OR [--mutationRetries=<retryCount>]       : Number of mutation retries. Defaults to zero.
    [-o <outputDir>] OR [--outputDir=<directory>]                : Specify directory to store differ runtime outputs. By default they are stored in the current working directory.
    [-c] OR [--clear]                                            : Clean before run.
    [-d] OR [--debugMode]                                        : Enable debug mode.
    [-w <setupTimeout>]                                          : Specify timeout duration.
    [--xattrExcludeKeysFile=<path/to/file>]                      : Path to the file containing xattr keys to exclude for comparison.
    [--help]                                                     : Show this help message and exit.

Example usage:
    ${BASH_SOURCE[0]} -h 127.0.0.1:8091 -u admin -p password -s sourceBucket -t targetBucket -r remoteCluster -c
    ${BASH_SOURCE[0]} --hostname=127.0.0.1:8091 --username=admin --password=password --sourceBucket=sourceBucketName --targetBucket=targetBucketName --remoteClusterName=RemoteRefName --clear

EOF
}

function waitForBgJobs {
	local mainPid=$1
	local mainPidCnt=$(ps -ef | grep -v grep | grep -c $mainPid)
	local jobsCnt=$(jobs -l | grep -c "Running")
	while (((($jobsCnt > 0)) && (($mainPidCnt > 0)))); do
		sleep 1
		jobsCnt=$(jobs -l | grep -c "Running")
		mainPidCnt=$(ps -ef | grep -v grep | grep -c $mainPid)
	done
}

function killBgTail {
	local tailPid=$(jobs -l | grep tail | awk '{print $2}')
	if [[ ! -z "$tailPid" ]]; then
		kill $tailPid >/dev/null 2>&1
	fi
}

while getopts ":h:p:u:r:s:t:cm:e:w:d:o:-:" opt; do
	case ${opt} in
	u)
		username=$OPTARG
		;;
	p)
		password=$OPTARG
		;;
	h)
		hostname=$OPTARG
		;;
	r)
		remoteClusterName=$OPTARG
		;;
	s)
		sourceBucketName=$OPTARG
		;;
	t)
		targetBucketName=$OPTARG
		;;
	c)
		cleanBeforeRun=1
		;;
	m)
		compareType=$OPTARG
		;;
	e)
		mutationRetries=$OPTARG
		;;
	d)
		debugMode=1
		;;
	w)
		setupTimeout=$OPTARG
		;;
	o)
		outputDirectory=$OPTARG
		;;
	-)
		case "${OPTARG}" in
		help)
			printHelp
			exit 0
			;;
		clear)
			cleanBeforeRun=1
			;;
		debugMode)
			debugMode=1
			;;
		username=*)
			username=${OPTARG#*=}
			;;
		password=*)
			password=${OPTARG#*=}
			;;
		hostname=*)
			hostname=${OPTARG#*=}
			;;
		sourceBucket=*)
			sourceBucketName=${OPTARG#*=}
			;;
		targetBucket=*)
			targetBucketName=${OPTARG#*=}
			;;
		remoteClusterName=*)
			remoteClusterName=${OPTARG#*=}
			;;
		compareType=*)
			compareType=${OPTARG#*=}
			;;
		mutationRetries=*)
			mutationRetries=${OPTARG#*=}
			;;
		xattrExcludeKeysFile=*)
			xattrExcludeKeysFile=${OPTARG#*=}
			;;
		outputDir=*)
			outputDirectory=${OPTARG#*=}
			;;
		*)
			echo "ERRO: Unknown option --${OPTARG}" >&2
			printHelp $0
			exit 1
			;;
		esac
		;;
	\?)
		echo "Invalid option: $OPTARG" 1>&2
		;;
	:)
		echo "Invalid option: $OPTARG requires an argument" 1>&2
		;;
	esac
done
shift $((OPTIND - 1))

if [[ -z "$outputDirectory" ]]; then
	cwd=$(pwd)
	outputDirectory="$cwd/outputs"
fi

mkdir -p $outputDirectory

sourceDir="$outputDirectory/source"
targetDir="$outputDirectory/target"
fileDiffDir="$outputDirectory/fileDiff"
mutationDiffDir="$outputDirectory/mutationDiff"
checkpointDir="$outputDirectory/checkpoint"
differLogFilePath="$outputDirectory/xdcrDiffer.log"

if [[ -z "$username" ]]; then
	echo "Missing username"
	printHelp
	exit 1
elif [[ -z "$password" ]]; then
	echo "Missing password"
	printHelp
	exit 1
elif [[ -z "$hostname" ]]; then
	echo "Missing hostname and port"
	printHelp
	exit 1
elif [[ -z "$sourceBucketName" ]]; then
	echo "Missing sourceBucket"
	printHelp
	exit 1
elif [[ -z "$targetBucketName" ]]; then
	echo "Missing targetBucket"
	printHelp
	exit 1
elif [[ -z "$remoteClusterName" ]] && [[ -z "$targetUrl" ]]; then
	echo "Missing remoteCluster name or target URL"
	printHelp
	exit 1
fi

findExec

export CBAUTH_REVRPC_URL="http://$username:$password@$hostname"

if [[ ! -z "$cleanBeforeRun" ]]; then
	echo "Cleaning up before run..."
	for directory in "$sourceDir" "$targetDir" "$fileDiffDir" "$mutationDiffDir" "$checkpointDir"; do
		rm -rf $directory
	done
fi

unameOut=$(uname)
maxFileDescs=""

if [[ "$unameOut" == "Linux" ]] || [[ "$unameOut" == "Darwin" ]]; then
	maxFileDescs=$(ulimit -n)
	if (($? == 0)) && [[ "$maxFileDescs" =~ ^[[:digit:]]+$ ]] && (($maxFileDescs > 4)); then
		# use 3/4 to prevent overrun
		maxFileDescs=$(echo $(($maxFileDescs / 4 * 3)))
	fi
fi

execString="$execGo"
execString="${execString} -sourceUrl"
execString="${execString} $hostname"
execString="${execString} -sourceUsername"
execString="${execString} $username"
execString="${execString} -sourcePassword"
execString="${execString} $password"
execString="${execString} -sourceBucketName"
execString="${execString} $sourceBucketName"
execString="${execString} -targetBucketName"
execString="${execString} $targetBucketName"

if [[ ! -z "$remoteClusterUsername" ]] && [[ ! -z "$remoteClusterPassword" ]]; then
	execString="${execString} -targetUsername"
	execString="${execString} $remoteClusterUsername"
	execString="${execString} -targetPassword"
	execString="${execString} $remoteClusterPassword"
fi
if [[ ! -z "$remoteClusterName" ]]; then
	execString="${execString} -remoteClusterName"
	execString="${execString} $remoteClusterName"
elif [[ ! -z "$targetUrl" ]]; then
	execString="${execString} -targetUrl"
	execString="${execString} $targetUrl"
fi
if [[ ! -z "$maxFileDescs" ]]; then
	execString="${execString} -numberOfFileDesc"
	execString="${execString} $maxFileDescs"
fi
if [[ ! -z "$compareType" ]]; then
	execString="${execString} -compareType"
	execString="${execString} $compareType"
fi
if [[ ! -z "$mutationRetries" ]]; then
	execString="${execString} -mutationRetries"
	execString="${execString} $mutationRetries"
fi
if [[ ! -z "$setupTimeout" ]]; then
	execString="${execString} -setupTimeout"
	execString="${execString} $setupTimeout"
fi
if [[ ! -z "$debugMode" ]]; then
	execString="${execString} -debugMode"
	execString="${execString} $debugMode"
fi
if [[ ! -z "$xattrExcludeKeysFile" ]]; then
	execString="${execString} -fileContaingXattrKeysForNoComapre"
	execString="${execString} $xattrExcludeKeysFile"
fi

execString="${execString} -sourceFileDir"
execString="${execString} $sourceDir"
execString="${execString} -targetFileDir"
execString="${execString} $targetDir"
execString="${execString} -fileDifferDir"
execString="${execString} $fileDiffDir"
execString="${execString} -mutationDifferDir"
execString="${execString} $mutationDiffDir"
execString="${execString} -checkpointFileDir"
execString="${execString} $checkpointDir"

# Execute the differ in background and watch the pid to be finished
$execString >$differLogFilePath 2>&1 &
bgPid=$(jobs -p)

# in the meantime, trap ctrl-c and pass the signal to the program
trap ctrl_c INT

function ctrl_c() {
	if [[ -z "$bgPid" ]]; then
		exit 0
	else
		kill -SIGINT $bgPid
		killBgTail
	fi
}

tail -f $differLogFilePath &
waitForBgJobs $bgPid
killBgTail

unset CBAUTH_REVRPC_URL
