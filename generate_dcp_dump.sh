#!/bin/bash

DIR=
ENTRIES=
MISMATCHES=0
NUM_VBUCKETS=1024
BIN_ENTRIES=1
NUM_MISSING_DOCS=0

#[keyLenBytes],[Key],[seqno],[CAS],[flags],[bodyHash]
#Body hash algorithm is TBD.
#For example:
#3,ABC,12,34,223,a1b2c3d4

while getopts ":d:n:m:b:" opt;do
    case $opt in
        n)
            echo "Number of entries per vbucket: $OPTARG"
            ENTRIES="$OPTARG"
            ;;
        d)
            echo "Dir specified: $OPTARG"
            DIR="$OPTARG"
            ;;
        m)
            echo "Number of mismatches: $OPTARG"
            MISMATCHES=$OPTARG
            ;;
        b)
            echo "Number of approximate entires per bin $OPTARG"
            BIN_ENTRIES=$OPTARG
            ;;
        x)
            echo "Number of missing documents on target $OPTARG"
            NUM_MISSING_DOCS=$OPTARG
            ;;
        \?)
            echo "Invalid option: -$OPTARG"
            exit 1
            ;;
        :)
            echo "Option $OPTARG requires an argument." 
            exit 1
            ;;
        esac
done

function usage {
cat << EOF
$0 <-d> output_dir <-n> entries per vbucket [-m Number of mismatches on target] [-b Approximate number of entries per bin]
EOF
}

function generateKeyLen {
    echo $((7 + RANDOM % 10))
}

function generateFlag {
    echo $((1 + RANDOM %10))
}

function generateKey {
    len=$1
    echo `openssl rand -base64 $len`
}

function generateHash {
    echo `openssl rand -base64 32`
}

#generate a large enough no for seqno and cas
function generateLargeNumber {
    completeSeqno=0
    for (( i=0; $i < 3; i=$(( $i + 1 )) ));do
        completeSeqno=$(( $completeSeqno * 100000 + $RANDOM ))
    done
    echo $completeSeqno
}

function outputSingleEntry {
    keyLen=`generateKeyLen`
    key=`generateKey $keyLen`
    seqno=`generateLargeNumber`
    CAS=`generateLargeNumber`
    flag=`generateFlag`
    bodyHash=`generateHash`
    expiry=`generateLargeNumber`

    echo "${keyLen},${key},${seqno},${CAS},${flag},${bodyHash},${expiry},"
}

function createDirectoryStructures {
    for (( i=0; $i < ${#clusterDir[@]}; i=$(( $i+1 )) ));do
        mkdir -p ${clusterDir[$i]}
        cd ${clusterDir[$i]}
        for (( j=0; $j < ${#vbucketDir[@]}; j=$(( $j+1 )) ));do
            mkdir -p ${vbucketDir[$j]}
        done
        cd ..
    done
}

function getNumEntriesPerBinFile {
    echo $(( $BIN_ENTRIES + $RANDOM % 10 ))
}

function getBinFileContent {
    local buffer
    entries=$1
    for (( i=0; $i < $entries; i=$(( $i+1 )) ));do
        buffer="${buffer}`outputSingleEntry`"
    done
    echo "$buffer"
}

function outputDataInBg {
    fileMinusCluster=$1
    shouldBeMismatched=$2
    shouldHaveMissingDoc=$3
    numEntries=`getNumEntriesPerBinFile`
    if (( $shouldBeMismatched == 0 ));then
        buffer=`getBinFileContent $numEntries`
        echo "$buffer" > ${clusterDir[0]}/$fileMinusCluster
        echo "$buffer" > ${clusterDir[1]}/$fileMinusCluster
    else
        echo "FILE $fileMinusCluster is mismatched"
        buffer=`getBinFileContent $numEntries`
        echo "$buffer" > ${clusterDir[0]}/$fileMinusCluster
        buffer=`getBinFileContent $numEntries`
        echo "$buffer" > ${clusterDir[1]}/$fileMinusCluster
    fi
}

function shouldBeMismatched {
    count=$1
    retVal=0
    for (( i=0; $i < ${#mismatchArray[@]}; i=$(( $i+1 )) ));do
        if (( ${mismatchArray[$i]} == $count ));then
            echo 1
            return
        fi
    done
    echo 0
}

function shouldHaveMissingDoc {
    count=$1
    retVal=0
    for (( i=0; $i < ${#missingDocArray[@]}; i=$(( $i+1 )) ));do
        if (( ${missingDocArray[$i]} == $count ));then
            echo 1
            return
        fi
    done
    echo 0

}

function createDCPDumpFiles {
    local binEntries
    local i
    local j
    local k
    local mismatchCnt=0
    echo "Creating Dump files... "
    for (( j=0; $j < ${#vbucketDir[@]}; j=$(( $j+1 )) ));do
        for (( k=0; $k < $ENTRIES; k=$(( $k+1 )) ));do
            mismatchOp=`shouldBeMismatched $mismatchCnt`
            missingOp=`shouldHaveMissingDoc $mismatchCnt`
            outputDataInBg "${vbucketDir[$j]}/$k" $mismatchOp $missingOp &
            mismatchCnt=$(( $mismatchCnt+1 ))
        done
    done
}

#MAIN
if [ -z $DIR ] || [ -z $ENTRIES ]; then
    usage
    exit 1
fi

#globals
declare -a clusterDir
declare -a vbucketDir
declare -a mismatchArray
declare -a missingDocArray
declare -i mismatchCounter=0

clusterDir[0]=c1
clusterDir[1]=c2
    
# Setup files or data structures needed
for (( i=0; $i < $NUM_VBUCKETS; i=(( $i+1 )) ));do
    vbucketDir[$i]=$i
done

totalNumOfEntries=$(( $NUM_VBUCKETS * $ENTRIES ))

for (( i=0; $i < $MISMATCHES; i=$(( $i+1 )) ));do
    oneMismatch=`jot -r 1 0 $totalNumOfEntries`
    mismatchArray[${#mismatchArray[@]}]=$oneMismatch
done
for (( i=0; $i < $NUM_MISSING_DOCS; i=$(( $i+1 )) ));do
    oneMissingDoc=`jot -r 1 0 $totalNumOfEntries`
    missingDocArray[${#missingDocArray[@]}]=$oneMissingDoc
done


#done Setup

mkdir -p $DIR && cd $DIR
createDirectoryStructures
createDCPDumpFiles
cd ..

