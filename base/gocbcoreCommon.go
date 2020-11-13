package base

import (
	"fmt"
	"github.com/couchbase/gocbcore/v9"
	"net/url"
	"strings"
	"time"
)

type GocbcoreAgentCommon struct {
	Name       string
	Servers    []string
	BucketName string

	SetupTimeout time.Duration
}

type PasswordAuth struct {
	Username string
	Password string
}

type RetryStrategy struct{}

func (rs *RetryStrategy) RetryAfter(req gocbcore.RetryRequest,
	reason gocbcore.RetryReason) gocbcore.RetryAction {
	if reason == gocbcore.BucketNotReadyReason {
		return &gocbcore.WithDurationRetryAction{
			WithDuration: gocbcore.ControlledBackoff(req.RetryAttempts()),
		}
	}

	return &gocbcore.NoRetryRetryAction{}
}

func GetConnStr(servers []string) string {
	// for now, http bootstrap only
	connStr := servers[0]
	if connURL, err := url.Parse(servers[0]); err == nil {
		if strings.HasPrefix(connURL.Scheme, "http") {
			// tack on an option: bootstrap_on=http for gocbcore SDK
			// connections to force HTTP config polling
			if ret, err := connURL.Parse("?bootstrap_on=http"); err == nil {
				connStr = ret.String()
			}
		}
	}
	return connStr
}

func TagHttpPrefix(url *string) {
	if !strings.HasPrefix(*url, "http") {
		*url = fmt.Sprintf("http://%v", *url)
	}
}
