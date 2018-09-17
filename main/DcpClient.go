package main

import (
	"fmt"
	"github.com/couchbase/gocb"
	gocbcore "gopkg.in/couchbase/gocbcore.v7"
	"math"
	"sync"
	"time"
)

type DcpClient struct {
	url             string
	bucketName      string
	userName        string
	password        string
	fileDir         string
	errChan         chan error
	waitGroup       *sync.WaitGroup
	numberOfWorkers int
	bucket          *gocb.StreamingBucket
	dcpHandlers     []*DcpHandler
	vbHandlerMap    map[uint16]*DcpHandler
	stopped         bool
	stateLock       sync.RWMutex
}

func NewDcpClient(url, bucketName, userName, password, fileDir string, numberOfWorkers int, errChan chan error, waitGroup *sync.WaitGroup) *DcpClient {
	return &DcpClient{
		url:             url,
		bucketName:      bucketName,
		userName:        userName,
		password:        password,
		fileDir:         fileDir,
		numberOfWorkers: numberOfWorkers,
		errChan:         errChan,
		waitGroup:       waitGroup,
		dcpHandlers:     make([]*DcpHandler, numberOfWorkers),
		vbHandlerMap:    make(map[uint16]*DcpHandler),
	}
}

func (c *DcpClient) Start() error {
	err := c.initialize()
	if err != nil {
		return err
	}

	// test. stop after 60 seconds
	go c.waitForStop()

	return c.openStreams()
}

func (c *DcpClient) waitForStop() {
	timer := time.NewTimer(60 * time.Second)
	select {
	case <-timer.C:
		c.Stop()
	}
}

func (c *DcpClient) Stop() error {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	if c.stopped {
		fmt.Printf("Skipping stop() because client is alreasy dtopped\n")
		return nil
	}

	defer c.waitGroup.Done()

	for i := 0; i < NumerOfVbuckets; i++ {
		_, err := c.bucket.IoRouter().CloseStream(uint16(i), c.closeStreamFunc)
		if err != nil {
			fmt.Printf("Error stopping dcp stream for vb %v. err=%v\n", i, err)
		}
	}

	err := c.bucket.IoRouter().Close()
	if err != nil {
		fmt.Printf("Error closing gocb agent. err=%v\n", err)
	}

	for _, dcpHandler := range c.dcpHandlers {
		dcpHandler.Stop()
	}

	c.stopped = true
	return nil
}

func (c *DcpClient) initialize() error {
	err := c.initializeBucket()
	if err != nil {
		fmt.Printf("Error getting gocb bucket. err=%v\n", err)
		return err
	}

	err = c.initializeDcpHandlers()
	if err != nil {
		fmt.Printf("Error initializing dcp handlers. err=%v\n", err)
		return err
	}

	return nil

}

func (c *DcpClient) openStreams() error {
	for i := 0; i < NumerOfVbuckets; i++ {
		_, err := c.bucket.IoRouter().OpenStream(uint16(i), 0, 0, 0, math.MaxInt32, 0, 0, c.vbHandlerMap[uint16(i)], c.openStreamFunc)
		if err != nil {
			fmt.Printf("err opening dcp stream for vb %v. err=%v\n", i, err)
			return err
		}
	}

	return nil
}

func (c *DcpClient) initializeBucket() (err error) {
	cluster, err := gocb.Connect(c.url)
	if err != nil {
		fmt.Printf("Error connecting to cluster. err=%v\n", err)
		return
	}
	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: c.userName,
		Password: c.password,
	})

	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	bucket, err := cluster.OpenStreamingBucket(StreamingBucketName, c.bucketName, "")
	if err != nil {
		fmt.Printf("Error opening streaming bucket. bucket=%v, err=%v\n", c.bucketName, err)
	}

	c.bucket = bucket

	return
}

func (c *DcpClient) initializeDcpHandlers() error {
	loadDistribution := BalanceLoad(c.numberOfWorkers, NumerOfVbuckets)
	for i := 0; i < c.numberOfWorkers; i++ {
		lowIndex := loadDistribution[i][0]
		highIndex := loadDistribution[i][1]
		vbList := make([]uint16, highIndex-lowIndex)
		for j := lowIndex; j < highIndex; j++ {
			vbList[j-lowIndex] = uint16(j)
		}

		dcpHandler, err := NewDcpHandler(c.fileDir, i, vbList)
		if err != nil {
			fmt.Printf("Error constructing dcp handler. err=%v\n", err)
			return err
		}

		err = dcpHandler.Start()
		if err != nil {
			fmt.Printf("Error starting dcp handler. err=%v\n", err)
			return err
		}

		c.dcpHandlers[i] = dcpHandler

		for j := lowIndex; j < highIndex; j++ {
			c.vbHandlerMap[uint16(j)] = dcpHandler
		}
	}
	return nil
}

// OpenStreamCallback
func (c *DcpClient) openStreamFunc(f []gocbcore.FailoverEntry, err error) {
	if err != nil {
		c.reportError(err)
	}
}

func (c *DcpClient) reportError(err error) {
	select {
	case c.errChan <- err:
	default:
		// some error already sent to errChan. no op
	}
}

// CloseStreamCallback
func (c *DcpClient) closeStreamFunc(err error) {
}
