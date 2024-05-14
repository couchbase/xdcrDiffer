package base

import (
	"crypto/tls"
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

type CertificateAuth struct {
	PasswordAuth
	CertificateBytes []byte
	PrivateKey       []byte
}

func (c *CertificateAuth) SupportsTLS() bool {
	return true
}

func (c *CertificateAuth) SupportsNonTLS() bool {
	return false
}

func (c *CertificateAuth) Certificate(req gocbcore.AuthCertRequest) (*tls.Certificate, error) {
	clientCert, err := tls.X509KeyPair(c.CertificateBytes, c.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("Invalid keypair")
	}
	return &clientCert, nil
}

func (c *CertificateAuth) Credentials(req gocbcore.AuthCredsRequest) ([]gocbcore.UserPassPair, error) {
	return []gocbcore.UserPassPair{{
		Username: c.Username,
		Password: c.Password,
	}}, nil
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
	if !strings.HasPrefix(*url, HttpPrefix) {
		*url = fmt.Sprintf("%v%v", HttpPrefix, *url)
	}
}

func TagHttpsPrefix(url *string) {
	if !strings.HasPrefix(*url, HttpsPrefix) {
		*url = fmt.Sprintf("%v%v", HttpsPrefix, *url)
	}
}

func TagCouchbaseSecurePrefix(url *string) {
	if !strings.HasPrefix(*url, CouchbaseSecurePrefix) {
		*url = fmt.Sprintf("%v%v", CouchbaseSecurePrefix, *url)
	}
}
