package differ

import (
	"crypto/x509"
	"fmt"
	"github.com/couchbase/gocbcore/v9"
	xdcrBase "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
	"reflect"
	"time"
	"xdcrDiffer/base"
)

type GocbcoreAgent struct {
	base.GocbcoreAgentCommon
	agent *gocbcore.Agent
}

func (a *GocbcoreAgent) setupAgent(auth interface{}, batchSize int, capability metadata.Capability) error {
	agentConfig, err := a.setupAgentConfig(auth, capability)
	if err != nil {
		return err
	}
	agentConfig.MaxQueueSize = batchSize * 50 // Give SDK some breathing room

	connStr := base.GetConnStr(a.Servers)

	err = agentConfig.FromConnStr(connStr)
	if err != nil {
		return err
	}

	return a.setupGocbcoreAgent(agentConfig)
}

func (a *GocbcoreAgent) setupAgentConfig(authIn interface{}, capability metadata.Capability) (*gocbcore.AgentConfig, error) {
	var auth gocbcore.AuthProvider
	var useTLS bool
	x509Provider := func() *x509.CertPool {
		return nil
	}
	if authIn != nil {
		if pwAuth, ok := authIn.(*base.PasswordAuth); ok {
			auth = gocbcore.PasswordAuthProvider{
				Username: pwAuth.Username,
				Password: pwAuth.Password,
			}
		} else if certAuth, ok := authIn.(*base.CertificateAuth); ok {
			useTLS = true
			auth = certAuth
			certPool := x509.NewCertPool()
			ok := certPool.AppendCertsFromPEM(certAuth.CertificateBytes)
			if !ok {
				return nil, xdcrBase.InvalidCerfiticateError
			}
			x509Provider = func() *x509.CertPool {
				return certPool
			}
		} else {
			panic(fmt.Sprintf("Unknown type: %v\n", reflect.TypeOf(authIn)))
		}
	}

	return &gocbcore.AgentConfig{
		HTTPAddrs:         a.Servers,
		BucketName:        a.BucketName,
		UserAgent:         a.Name,
		Auth:              auth,
		UseCollections:    capability.HasCollectionSupport(),
		ConnectTimeout:    a.SetupTimeout,
		KVConnectTimeout:  a.SetupTimeout,
		UseTLS:            useTLS,
		TLSRootCAProvider: x509Provider,
	}, nil
}

func (a *GocbcoreAgent) setupGocbcoreAgent(config *gocbcore.AgentConfig) (err error) {
	a.agent, err = gocbcore.CreateAgent(config)
	if err != nil {
		return err
	}

	options := gocbcore.WaitUntilReadyOptions{
		DesiredState:  gocbcore.ClusterStateOnline,
		ServiceTypes:  []gocbcore.ServiceType{gocbcore.MemdService},
		RetryStrategy: &base.RetryStrategy{},
	}

	signal := make(chan error, 1)
	_, err = a.agent.WaitUntilReady(time.Now().Add(a.SetupTimeout),
		options, func(res *gocbcore.WaitUntilReadyResult, er error) {
			signal <- er
		})

	if err == nil {
		err = <-signal
	}

	if err != nil {
		go a.agent.Close()
	}
	return
}

func (a *GocbcoreAgent) Get(key string, callbackFunc func(result *gocbcore.GetResult, err error), colId uint32) error {
	opts := gocbcore.GetOptions{
		Key:           []byte(key),
		RetryStrategy: nil,
		CollectionID:  colId,
	}
	_, err := a.agent.Get(opts, callbackFunc)
	return err
}

func NewGocbcoreAgent(id string, servers []string, bucketName string, auth interface{}, batchSize int, capability metadata.Capability) (*GocbcoreAgent, error) {
	gocbcoreAgent := &GocbcoreAgent{
		GocbcoreAgentCommon: base.GocbcoreAgentCommon{
			Name:         id,
			Servers:      servers,
			BucketName:   bucketName,
			SetupTimeout: 5 * time.Second,
		},
		agent: nil,
	}

	err := gocbcoreAgent.setupAgent(auth, batchSize, capability)
	return gocbcoreAgent, err
}
