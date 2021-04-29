package dcp

import (
	gocbcore "github.com/couchbase/gocbcore/v9"
	memd "github.com/couchbase/gocbcore/v9/memd"
	"time"
	"xdcrDiffer/base"
)

type GocbcoreDCPFeed struct {
	base.GocbcoreAgentCommon
	dcpAgent *gocbcore.DCPAgent
}

func (f *GocbcoreDCPFeed) setupDCPAgent(pw *base.PasswordAuth) error {
	agentConfig := f.setupDCPAgentConfig(pw)

	connStr := base.GetConnStr(f.Servers)

	err := agentConfig.FromConnStr(connStr)
	if err != nil {
		return err
	}

	dcpFeedParams := NewDCPFeedParams()

	flags := memd.DcpOpenFlagProducer
	if dcpFeedParams.IncludeXAttrs {
		flags |= memd.DcpOpenFlagIncludeXattrs
	}

	if dcpFeedParams.NoValue {
		flags |= memd.DcpOpenFlagNoValue
	}

	err = f.setupGocbcoreDCPAgent(agentConfig, flags)
	return err
}

type DCPFeedParams struct {
	// Used to specify whether the applications are interested
	// in receiving the xattrs information in a dcp stream.
	IncludeXAttrs bool `json:"includeXAttrs,omitempty"`

	// Used to specify whether the applications are not interested
	// in receiving the value for mutations in a dcp stream.
	NoValue bool `json:"noValue,omitempty"`

	// Scope within the bucket to stream data from.
	Scope string `json:"scope,omitempty"`

	// Collections within the scope that the feed would cover.
	Collections []string `json:"collections,omitempty"`
}

func NewDCPFeedParams() *DCPFeedParams {
	return &DCPFeedParams{IncludeXAttrs: true}
}

func (f *GocbcoreDCPFeed) setupDCPAgentConfig(pw *base.PasswordAuth) *gocbcore.DCPAgentConfig {
	var auth gocbcore.AuthProvider
	if pw != nil {
		auth = gocbcore.PasswordAuthProvider{
			Username: pw.Username,
			Password: pw.Password,
		}
	}

	return &gocbcore.DCPAgentConfig{
		UserAgent:        f.Name,
		BucketName:       f.BucketName,
		Auth:             auth,
		ConnectTimeout:   f.SetupTimeout,
		KVConnectTimeout: f.SetupTimeout,
		UseCollections:   true,
		// DCPBufferSize
	}
}

func (f *GocbcoreDCPFeed) setupGocbcoreDCPAgent(config *gocbcore.DCPAgentConfig, flags memd.DcpOpenFlag) (err error) {
	f.dcpAgent, err = gocbcore.CreateDcpAgent(config, f.Name, flags)
	if err != nil {
		return err
	}

	options := gocbcore.WaitUntilReadyOptions{
		DesiredState:  gocbcore.ClusterStateOnline,
		ServiceTypes:  []gocbcore.ServiceType{gocbcore.MemdService},
		RetryStrategy: &base.RetryStrategy{},
	}

	signal := make(chan error, 1)
	_, err = f.dcpAgent.WaitUntilReady(time.Now().Add(f.SetupTimeout),
		options, func(res *gocbcore.WaitUntilReadyResult, er error) {
			signal <- er
		})

	if err == nil {
		err = <-signal
	}

	if err != nil {
		go f.dcpAgent.Close()
	}
	return
}

func NewGocbcoreDCPFeed(id string, servers []string, bucketName string, passwordAuth *base.PasswordAuth) (*GocbcoreDCPFeed, error) {
	gocbcoreDcpFeed := &GocbcoreDCPFeed{
		GocbcoreAgentCommon: base.GocbcoreAgentCommon{
			Name:         id,
			Servers:      servers,
			BucketName:   bucketName,
			SetupTimeout: 5 * time.Second,
		},
		dcpAgent: nil,
	}

	err := gocbcoreDcpFeed.setupDCPAgent(passwordAuth)
	return gocbcoreDcpFeed, err
}
