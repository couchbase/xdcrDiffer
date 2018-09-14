package main

import (
	"flag"
	"fmt"
	"os"
)

var options struct {
	numVBucket       uint
	numBinPerVBucket uint
}

func argParse() {
	flag.UintVar(&options.numVBucket, "numVB", 1024, "Number of vBuckets")
	flag.UintVar(&options.numBinPerVBucket, "numBin", 5, "Number of bin files per vBucket")

	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	argParse()
}
