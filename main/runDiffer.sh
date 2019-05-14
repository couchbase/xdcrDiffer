#!/bin/bash

run_args=$@

execGo="./main"

function printHelp() {
cat << EOF
Usage: $0 -n <username> -p <password> -u <url:port> -r <remoteClusterName> -s <sourceBucket> -t <targetBucket>
Example: $0 -n Administrator -p password -u 127.0.0.1:9000
EOF
}

while getopts ":u:p:n:r:s:t:" opt; do
  case ${opt} in
    n )
      username=$OPTARG
      ;;
    p )
      password=$OPTARG
      ;;
    u )
      url=$OPTARG
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
elif [[ -z "$url" ]];then
	echo "Missing url"
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

# separate url and port
if [[ "$url" =~ (.*):([0-9]+) ]];then
	echo "URL: $url port: $port"
else
	echo "Did not find port in url"
	exit 1
fi

export CBAUTH_REVRPC_URL="http://$username:$password@$url"
echo "Exporting $CBAUTH_REVRPC_URL"

$execGo -sourceUrl $hostname:$port -sourceUsername $username -sourcePassword $password -sourceBucketName $sourceBucketName -targetBucketName $targetBucketName -remoteClusterName $remoteClusterName

unset CBAUTH_REVRPC_URL
