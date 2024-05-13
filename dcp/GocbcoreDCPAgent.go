package dcp

import (
	"crypto/x509"
	"fmt"
	"github.com/couchbase/goxdcr/metadata"
	"reflect"
	"time"
	"xdcrDiffer/base"

	gocbcore "github.com/couchbase/gocbcore/v9"
	memd "github.com/couchbase/gocbcore/v9/memd"
	xdcrBase "github.com/couchbase/goxdcr/base"
)

type GocbcoreDCPFeed struct {
	base.GocbcoreAgentCommon
	dcpAgent *gocbcore.DCPAgent
}

func (f *GocbcoreDCPFeed) setupDCPAgent(auth interface{}, collections bool, ref *metadata.RemoteClusterReference) error {
	agentConfig, shouldBeSecure, err := f.setupDCPAgentConfig(auth, collections, ref)
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

func (f *GocbcoreDCPFeed) setupDCPAgentConfig(authMech interface{}, collections bool, ref *metadata.RemoteClusterReference) (*gocbcore.DCPAgentConfig, bool, error) {
	useTLS, x509Provider, auth, err := getAgentConfigs(authMech, ref)
	if err != nil {
		return nil, false, err
	}
	if auth == nil {
		panic("Nil auth")
	}
	return &gocbcore.DCPAgentConfig{
		UserAgent:         f.Name,
		BucketName:        f.BucketName,
		Auth:              auth,
		ConnectTimeout:    f.SetupTimeout,
		KVConnectTimeout:  f.SetupTimeout,
		UseCollections:    collections,
		UseTLS:            useTLS,
		TLSRootCAProvider: x509Provider,
	}, useTLS, nil
}

func getAgentConfigs(authMech interface{}, ref *metadata.RemoteClusterReference) (bool, func() *x509.CertPool, gocbcore.AuthProvider, error) {
	certPool := x509.NewCertPool()
	var useTLS bool

	if ref.HttpAuthMech() == xdcrBase.HttpAuthMechHttps {
		// https means we need to at a min return a root CA
		ok := certPool.AppendCertsFromPEM(ref.Certificate())
		if !ok {
			return false, nil, nil, fmt.Errorf("Invalid rootCA %s", ref.Certificate())
		}
		useTLS = true
	}
	x509Provider := func() *x509.CertPool {
		return certPool
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
		ok = certPool.AppendCertsFromPEM(cert.CertificateBytes)
		if !ok {
			return useTLS, nil, nil, fmt.Errorf("Invalid client cert %s", cert.CertificateBytes)
		}
		ok = certPool.AppendCertsFromPEM(cert.PrivateKey)
		if !ok {
			return useTLS, nil, nil, fmt.Errorf("Invalid client key %s", cert.PrivateKey)
		}
	} else {
		panic(fmt.Sprintf("unknown authmech: %v", reflect.TypeOf(authMech)))
	}
	return useTLS, x509Provider, auth, nil
}

func (f *GocbcoreDCPFeed) setupGocbcoreDCPAgent(config *gocbcore.DCPAgentConfig, flags memd.DcpOpenFlag, secure bool) (err error) {
	f.dcpAgent, err = gocbcore.CreateDcpAgent(config, f.Name, flags)
	if err != nil {
		return
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
		errClosing := f.dcpAgent.Close()
		err = fmt.Errorf("Closing GocbcoreDCPFeed.agent because of err=%v, error while closing=%v", err, errClosing)
		return
	}

	if secure && !f.dcpAgent.IsSecure() {
		err = fmt.Errorf("%v requested secure but agent says not secure", f.Name)
		return
	}

	return
}

func NewGocbcoreDCPFeed(id string, servers []string, bucketName string, auth interface{}, collections bool, ref *metadata.RemoteClusterReference) (*GocbcoreDCPFeed, error) {
	gocbcoreDcpFeed := &GocbcoreDCPFeed{
		GocbcoreAgentCommon: base.GocbcoreAgentCommon{
			Name:         id,
			Servers:      servers,
			BucketName:   bucketName,
			SetupTimeout: time.Duration(base.SetupTimeoutSeconds) * time.Second,
		},
		dcpAgent: nil,
	}

	if auth == nil {
		panic("nil auth")
	}

	err := gocbcoreDcpFeed.setupDCPAgent(auth, collections, ref)
	return gocbcoreDcpFeed, err
}
