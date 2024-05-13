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

func (a *GocbcoreAgent) setupAgent(auth interface{}, batchSize int, capability metadata.Capability, reference *metadata.RemoteClusterReference) error {
	agentConfig, err := a.setupAgentConfig(auth, capability, reference)
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

func (a *GocbcoreAgent) setupAgentConfig(authIn interface{}, capability metadata.Capability, reference *metadata.RemoteClusterReference) (*gocbcore.AgentConfig, error) {
	var auth gocbcore.AuthProvider
	var useTLS bool
	certPool := x509.NewCertPool()

	if authIn == nil {
		panic("authIn is nil")
	}

	if reference.HttpAuthMech() == xdcrBase.HttpAuthMechHttps {
		useTLS = true
		ok := certPool.AppendCertsFromPEM(reference.Certificate())
		if !ok {
			return nil, fmt.Errorf("Invalid rootCA from gocbcoreagent")
		}
	}
	x509Provider := func() *x509.CertPool {
		return certPool
	}

	if pwAuth, ok := authIn.(*base.PasswordAuth); ok {
		auth = gocbcore.PasswordAuthProvider{
			Username: pwAuth.Username,
			Password: pwAuth.Password,
		}
	} else if certAuth, ok := authIn.(*base.CertificateAuth); ok {
		useTLS = true
		auth = certAuth
		ok = certPool.AppendCertsFromPEM(certAuth.CertificateBytes)
		if !ok {
			return nil, fmt.Errorf("setupAgent invalid clientCert %s", certAuth.CertificateBytes)
		}
		ok = certPool.AppendCertsFromPEM(certAuth.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("setupAgent invalid clientKey %s", certAuth.PrivateKey)
		}
	} else {
		panic(fmt.Sprintf("Unknown type: %v\n", reflect.TypeOf(authIn)))
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
		errClosing := a.agent.Close()
		err = fmt.Errorf("Closing GocbcoreAgent.agent because of err=%v, error while closing=%v", err, errClosing)
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

func (a *GocbcoreAgent) GetMeta(key string, callbackFunc func(result *gocbcore.GetMetaResult, err error), colId uint32) error {
	opts := gocbcore.GetMetaOptions{
		Key:           []byte(key),
		RetryStrategy: nil,
		CollectionID:  colId,
	}
	_, err := a.agent.GetMeta(opts, callbackFunc)
	return err
}

func NewGocbcoreAgent(id string, servers []string, bucketName string, auth interface{}, batchSize int, capability metadata.Capability, reference *metadata.RemoteClusterReference) (*GocbcoreAgent, error) {
	gocbcoreAgent := &GocbcoreAgent{
		GocbcoreAgentCommon: base.GocbcoreAgentCommon{
			Name:         id,
			Servers:      servers,
			BucketName:   bucketName,
			SetupTimeout: time.Duration(base.SetupTimeoutSeconds) * time.Second,
		},
		agent: nil,
	}

	err := gocbcoreAgent.setupAgent(auth, batchSize, capability, reference)
	return gocbcoreAgent, err
}
