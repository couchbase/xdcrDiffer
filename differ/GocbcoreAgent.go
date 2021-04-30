package differ

import (
	"github.com/couchbase/gocbcore/v9"
	"time"
	"xdcrDiffer/base"
)

type GocbcoreAgent struct {
	base.GocbcoreAgentCommon
	agent *gocbcore.Agent
}

func (a *GocbcoreAgent) setupAgent(password *base.PasswordAuth, batchSize int) error {
	agentConfig := a.setupAgentConfig(password)
	agentConfig.MaxQueueSize = batchSize * 50 // Give SDK some breathing room

	connStr := base.GetConnStr(a.Servers)

	err := agentConfig.FromConnStr(connStr)
	if err != nil {
		return err
	}

	return a.setupGocbcoreAgent(agentConfig)
}

func (a *GocbcoreAgent) setupAgentConfig(pw *base.PasswordAuth) *gocbcore.AgentConfig {
	var auth gocbcore.AuthProvider
	if pw != nil {
		auth = gocbcore.PasswordAuthProvider{
			Username: pw.Username,
			Password: pw.Password,
		}
	}

	return &gocbcore.AgentConfig{
		HTTPAddrs:        a.Servers,
		BucketName:       a.BucketName,
		UserAgent:        a.Name,
		Auth:             auth,
		UseCollections:   false,
		ConnectTimeout:   a.SetupTimeout,
		KVConnectTimeout: a.SetupTimeout,
	}
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

func (a *GocbcoreAgent) Get(key string, scopeName string, collectionName string, callbackFunc func(result *gocbcore.GetResult, err error)) error {
	opts := gocbcore.GetOptions{
		Key:            []byte(key),
		CollectionName: collectionName,
		ScopeName:      scopeName,
		RetryStrategy:  nil,
	}
	_, err := a.agent.Get(opts, callbackFunc)
	return err
}

func NewGocbcoreAgent(id string, servers []string, bucketName string, password *base.PasswordAuth, batchSize int) (*GocbcoreAgent, error) {
	gocbcoreAgent := &GocbcoreAgent{
		GocbcoreAgentCommon: base.GocbcoreAgentCommon{
			Name:         id,
			Servers:      servers,
			BucketName:   bucketName,
			SetupTimeout: 5 * time.Second,
		},
		agent: nil,
	}

	err := gocbcoreAgent.setupAgent(password, batchSize)
	return gocbcoreAgent, err
}
