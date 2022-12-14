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

execGo="xdcrDiffer"
differLogFileName="${execGo}.log"

function findExec() {
	if [[ ! -f "$execGo" ]]; then
		echo "Unable to find xdcr diff tool. Did you run make?"
		exit 1
	fi
}

function printHelp() {
	findExec

	cat <<EOF
Usage: $0 -u <username> -p <password> -h <hostname:port> [-c clean] 

This script will set up the necessary environment variable to allow the XDCR diff tool to connect to the metakv service in the
specified source cluster (NOTE: over http://) and retrieve the specified replication spec and start the tool in observation mode.

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

while getopts ":h:p:u:c" opt; do
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
	c)
		cleanBeforeRun=1
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
fi

findExec

export CBAUTH_REVRPC_URL="http://$username:$password@$hostname/xdcrDifferObserve"
echo "Exporting $CBAUTH_REVRPC_URL"

if [[ ! -z "$cleanBeforeRun" ]]; then
	echo "Cleaning up before run..."
	for directory in "source target fileDiff mutationDiff checkpoint"; do
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

currentPwd=$(pwd)
execString="$currentPwd/$execGo"
execString="${execString} -sourceUrl"
execString="${execString} $hostname"
execString="${execString} -sourceUsername"
execString="${execString} $username"
execString="${execString} -sourcePassword"
execString="${execString} $password"
if [[ ! -z "$maxFileDescs" ]]; then
	execString="${execString} -numberOfFileDesc"
	execString="${execString} $maxFileDescs"
fi

# Execute the differ in background and watch the pid to be finished
$execString >$differLogFileName 2>&1 &
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

tail -f $differLogFileName &
waitForBgJobs $bgPid
killBgTail

unset CBAUTH_REVRPC_URL
