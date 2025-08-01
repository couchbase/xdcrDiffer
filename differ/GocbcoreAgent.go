package differ

import (
	"crypto/x509"
	"fmt"
	"reflect"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	xdcrBase "github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/xdcrDiffer/base"
)

type GocbcoreAgent struct {
	base.GocbcoreAgentCommon
	agent *gocbcore.Agent
}

func (a *GocbcoreAgent) setupAgent(auth interface{}, batchSize int, capability metadata.Capability, reference *metadata.RemoteClusterReference) error {
	agentConfig, err := a.setupAgentConfig(auth, capability, batchSize, reference)
	if err != nil {
		return err
	}

	connStr := base.GetConnStr(a.Servers)

	err = agentConfig.FromConnStr(connStr)
	if err != nil {
		return err
	}

	return a.setupGocbcoreAgent(agentConfig)
}

func (a *GocbcoreAgent) setupAgentConfig(authIn interface{}, capability metadata.Capability, batchSize int, reference *metadata.RemoteClusterReference) (*gocbcore.AgentConfig, error) {
	var auth gocbcore.AuthProvider
	var useTLS bool
	certPool := x509.NewCertPool()

	if authIn == nil {
		panic("authIn is nil")
	}

	if reference.HttpAuthMech() == xdcrBase.HttpAuthMechHttps {
		useTLS = true
		ok := certPool.AppendCertsFromPEM(reference.Certificates())
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
		auth = certAuth
		ok = certPool.AppendCertsFromPEM(certAuth.CertificateBytes)
		if !ok {
			return nil, fmt.Errorf("setupAgent invalid clientCert %s", certAuth.CertificateBytes)
		}
	} else {
		panic(fmt.Sprintf("Unknown type: %v\n", reflect.TypeOf(authIn)))
	}

	return &gocbcore.AgentConfig{
		SeedConfig: gocbcore.SeedConfig{MemdAddrs: a.Servers},
		BucketName: a.BucketName,
		UserAgent:  a.Name,
		SecurityConfig: gocbcore.SecurityConfig{
			UseTLS:            useTLS,
			TLSRootCAProvider: x509Provider,
			Auth:              auth,
			AuthMechanisms:    base.ScramShaAuth,
		},
		KVConfig: gocbcore.KVConfig{
			ConnectTimeout: a.SetupTimeout,
			MaxQueueSize:   batchSize * 50, // Give SDK some breathing room
		},
		CompressionConfig: gocbcore.CompressionConfig{Enabled: true},
		HTTPConfig:        gocbcore.HTTPConfig{ConnectTimeout: a.SetupTimeout},
		IoConfig:          gocbcore.IoConfig{UseCollections: capability.HasCollectionSupport()},
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

func (a *GocbcoreAgent) GetHlv(key string, callbackFunc func(result *gocbcore.LookupInResult, err error), colId uint32) error {
	opts := gocbcore.LookupInOptions{
		Key:   []byte(key),
		Flags: memd.SubdocDocFlagAccessDeleted,
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpType(memd.CmdSubDocGet),
				Flags: memd.SubdocFlag(xdcrBase.SUBDOC_FLAG_XATTR),
				Path:  xdcrBase.XATTR_HLV,
				Value: nil,
			},
			{
				Op:    memd.SubDocOpType(memd.CmdSubDocGet),
				Flags: memd.SubdocFlag(xdcrBase.SUBDOC_FLAG_XATTR),
				Path:  xdcrBase.XATTR_MOU,
				Value: nil,
			},
		},
		RetryStrategy: nil,
		CollectionID:  colId,
	}
	_, err := a.agent.LookupIn(opts, callbackFunc)
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
