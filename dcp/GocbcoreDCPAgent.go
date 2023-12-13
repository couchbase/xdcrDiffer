package dcp

import (
	"crypto/x509"
	"fmt"
	gocbcore "github.com/couchbase/gocbcore/v10"
	memd "github.com/couchbase/gocbcore/v10/memd"
	xdcrBase "github.com/couchbase/goxdcr/base"
	"time"
	"xdcrDiffer/base"
)

type GocbcoreDCPFeed struct {
	base.GocbcoreAgentCommon
	dcpAgent *gocbcore.DCPAgent
}

func (f *GocbcoreDCPFeed) setupDCPAgent(auth interface{}, collections bool) error {
	agentConfig, shouldBeSecure, err := f.setupDCPAgentConfig(auth, collections)
	if err != nil {
		return err
	}

	connStr := base.GetConnStr(f.Servers)

	err = agentConfig.FromConnStr(connStr)
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

	err = f.setupGocbcoreDCPAgent(agentConfig, flags, shouldBeSecure)
	return err
}

type DCPFeedParams struct {
	// Used to specify whether the applications are interested
	// in receiving the xattrs information in a dcp stream.
	IncludeXAttrs bool `json:"includeXAttrs,omitempty"`

	// Used to specify whether the applications are not interested
	// in receiving the Value for mutations in a dcp stream.
	NoValue bool `json:"noValue,omitempty"`

	// Scope within the bucket to stream data from.
	Scope string `json:"scope,omitempty"`

	// Collections within the scope that the feed would cover.
	Collections []string `json:"collections,omitempty"`
}

func NewDCPFeedParams() *DCPFeedParams {
	return &DCPFeedParams{IncludeXAttrs: true}
}

func (f *GocbcoreDCPFeed) setupDCPAgentConfig(authMech interface{}, collections bool) (*gocbcore.DCPAgentConfig, bool, error) {
	useTLS, x509Provider, auth, err := getAgentConfigs(authMech)
	if err != nil {
		return nil, false, err
	}

	return &gocbcore.DCPAgentConfig{
		UserAgent:  f.Name,
		BucketName: f.BucketName,
		SecurityConfig: gocbcore.SecurityConfig{
			UseTLS:            useTLS,
			TLSRootCAProvider: x509Provider,
			Auth:              auth,
			AuthMechanisms:    base.ScramShaAuth,
		},
		KVConfig: gocbcore.KVConfig{
			ConnectTimeout: f.SetupTimeout,
		},
		CompressionConfig: gocbcore.CompressionConfig{Enabled: true},
		IoConfig:          gocbcore.IoConfig{UseCollections: collections},
		HTTPConfig:        gocbcore.HTTPConfig{ConnectTimeout: f.SetupTimeout},
	}, useTLS, nil
}

func getAgentConfigs(authMech interface{}) (bool, func() *x509.CertPool, gocbcore.AuthProvider, error) {
	var useTLS bool
	x509Provider := func() *x509.CertPool {
		return nil
	}
	var auth gocbcore.AuthProvider
	if pw, ok := authMech.(*base.PasswordAuth); ok {
		auth = gocbcore.PasswordAuthProvider{
			Username: pw.Username,
			Password: pw.Password,
		}
	} else if cert, ok := authMech.(*base.CertificateAuth); ok {
		// The base.CertificateAuth should implement the methods for a provider
		auth = cert
		useTLS = true
		certPool := x509.NewCertPool()
		ok := certPool.AppendCertsFromPEM(cert.CertificateBytes)
		if !ok {
			return false, nil, nil, xdcrBase.InvalidCerfiticateError
		}
		x509Provider = func() *x509.CertPool {
			return certPool
		}
	}
	return useTLS, x509Provider, auth, nil
}

func (f *GocbcoreDCPFeed) setupGocbcoreDCPAgent(config *gocbcore.DCPAgentConfig, flags memd.DcpOpenFlag, secure bool) (err error) {
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

	if secure && !f.dcpAgent.IsSecure() {
		return fmt.Errorf("%v requested secure but agent says not secure", f.Name)
	}

	return
}

func NewGocbcoreDCPFeed(id string, servers []string, bucketName string, auth interface{}, collections bool) (*GocbcoreDCPFeed, error) {
	gocbcoreDcpFeed := &GocbcoreDCPFeed{
		GocbcoreAgentCommon: base.GocbcoreAgentCommon{
			Name:         id,
			Servers:      servers,
			BucketName:   bucketName,
			SetupTimeout: base.SetupTimeout,
		},
		dcpAgent: nil,
	}

	err := gocbcoreDcpFeed.setupDCPAgent(auth, collections)
	return gocbcoreDcpFeed, err
}
