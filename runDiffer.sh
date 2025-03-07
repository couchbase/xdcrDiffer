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
	OR
	${BASH_SOURCE[0]} -y /path/to/config.yaml

Options:
	-h <host:port> OR --hostname=<host:port>                     : Specify Couchbase server hostname and port number.
	-p <password> OR --password=<password>                       : Specify Couchbase server password.
	-u <username> OR --username=<username>                       : Specify Couchbase server username.
	-r <remoteClusterName> OR --remoteClusterName=<name>         : Specify the remote cluster name.
	-s <sourceBucket> OR --sourceBucket=<bucket>                 : Specify the source bucket.
	-t <targetBucket> OR --targetBucket=<bucket>                 : Specify the target bucket.
	[-m <meta|body|both>] OR [--compareType=<meta|body|both>]    : Specify the type of comparison to perform (meta, body, both). By default it is meta.
	[-e <retryCount>]  OR [--mutationRetries=<retryCount>]       : Number of mutation retries. Defaults to zero.
	[-o <outputDir>] OR [--outputDir=<directory>]                : Specify directory to store differ runtime outputs. By default they are stored within the outputs directory in the current working directory.
	[-c] OR [--clear]                                            : Clean before run.
	[-d] OR [--debugMode]                                        : Enable debug mode.
	[-y <path/to/yaml>] OR [--yamlFile=<path/to/yaml>]           : Specify the path to the yaml file containing the configuration.
	[-w <setupTimeout>]                                          : Specify timeout duration.
	[--xattrExcludeKeysFile=<path/to/file>]                      : Path to the file containing xattr keys to exclude for comparison.
	[--newCkptFile=<path/to/file>]                               : Path to the new checkpoint file.
	[--oldCkptFile=<path/to/file>]                               : Path to the old checkpoint file.
	[--ckptInterval=<interval>]                                  : Checkpoint interval in seconds.
	[--help]                                                     : Show this help message and exit.

Example usage:
	${BASH_SOURCE[0]} -h 127.0.0.1:8091 -u admin -p password -s sourceBucket -t targetBucket -r remoteCluster -c
	${BASH_SOURCE[0]} --hostname=127.0.0.1:8091 --username=admin --password=password --sourceBucket=sourceBucketName --targetBucket=targetBucketName --remoteClusterName=RemoteRefName --clear
	${BASH_SOURCE[0]} -y ./sampleConfig.yaml
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

while getopts ":h:p:u:r:s:t:cm:e:w:d:o:y:-:" opt; do
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
	y)
		yamlFile=$OPTARG
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

		newCkptFile=*)
			newCkptFile=${OPTARG#*=}
			;;
		oldCkptFile=*)
			oldCkptFile=${OPTARG#*=}
			;;
		ckptInterval=*)
			ckptInterval=${OPTARG#*=}
			;;
		yamlFile=*)
			yamlFile=${OPTARG#*=}
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

differLogFilePath=""
execString=""

function clearBeforeRun {
	local clean=$1
	local outDir=$2
	if [[ ! -z "$clean" ]]; then
		echo "Cleaning up before run..."
		for directory in "source" "target" "fileDiff" "mutationDiff" "checkpoint" "xdcrDiffer.log"; do
			rm -rf "$outDir/$directory"
		done
	fi
}

function setupFromCmdLine {

	if [[ -z "$outputDirectory" ]]; then
		cwd=$(pwd)
		outputDirectory="$cwd/outputs"
	fi

	# If the specifed directory does not exist then create one.
	# If already present then its a no-op
	mkdir -p $outputDirectory

	# clears existing data if any based on the flag set
	clearBeforeRun $cleanBeforeRun $outputDirectory

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
	if [[ ! -z "$newCkptFile" ]]; then
		execString="${execString} -newCheckpointFileName"
		execString="${execString} $newCkptFile"
	fi
	if [[ ! -z "$oldCkptFile" ]]; then
		execString="${execString} -oldCheckpointFileName"
		execString="${execString} $oldCkptFile"
	fi
	if [[ ! -z "$ckptInterval" ]]; then
		execString="${execString} -checkpointInterval"
		execString="${execString} $ckptInterval"
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

	if [[ @PRODUCT_VERSION@ != @* ]]; then
		if [[ "${outputDirectory}" == /opt/couchbase* ]]; then
			echo "outputDir should be specified and should not be under /opt/couchbase"
			exit 2
		fi
	fi
}

function setupFromYaml {
	# Check if yq is installed
	if ! command -v yq &>/dev/null; then
		echo "Error: yq is not installed. Please install it first."
		exit 1
	fi

	hostname=$(yq eval '.sourceUrl' $yamlFile)
	username=$(yq eval '.sourceUsername' $yamlFile)
	password=$(yq eval '.sourcePassword' $yamlFile)
	clear=$(yq eval '.clearBeforeRun' $yamlFile)
	outDir=$(yq eval '.outputFileDir' $yamlFile)

	# If the specifed directory does not exist then create one.
	# If already present then its a no-op
	mkdir -p $outDir

	# clears existing data if any based on the flag set
	clearBeforeRun $clear $outDir

	findExec

	export CBAUTH_REVRPC_URL="http://$username:$password@$hostname"

	execString="$execGo"
	execString="${execString} -yamlConfigFilePath"
	execString="${execString} $yamlFile"

	differLogFilePath="$outDir/xdcrDiffer.log"
}

if [[ ! -z "$yamlFile" ]]; then
	setupFromYaml
else
	setupFromCmdLine
fi

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
