package security

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/jcmturner/gokrb5/v8"
	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/spnego"
)

// KerberosAuth handles Kerberos authentication for HDFS and enterprise databases
type KerberosAuth struct {
	principal      string
	realm          string
	kdc            string
	keytab         []byte
	config         *config.Config
}

// NewKerberosAuthFromKeytab creates a Kerberos authenticator from keytab data
func NewKerberosAuthFromKeytab(principal, realm string, keytabData []byte) (*KerberosAuth, error) {
	if keytabData == nil {
		return nil, fmt.Errorf("keytab data is required")
	}

	// Load keytab
	kt, err := keytab.Load(keytabData)
	if err != nil {
		return nil, fmt.Errorf("failed to load keytab: %w", err)
	}

	return &KerberosAuth{
		principal: principal,
		realm:     realm,
		keytab:    keytabData,
		config:    &config.Config{
			Realm: realm,
			Krb5Config: &config.Krb5Config{
				DefaultRealm: realm,
				Realms: map[string]*config.Realm{
					realm: {
						KDC:  []string{"*"}, // Will be set if specified
					},
				},
			},
		},
	}, nil
}

// NewKerberosAuthFromPassword creates a Kerberos authenticator from username/password
func NewKerberosAuthFromPassword(username, password, realm string) (*KerberosAuth, error) {
	return &KerberosAuth{
		principal: username,
		realm:     realm,
		config: &config.Config{
			Realm: realm,
			Krb5Config: &config.Krb5Config{
				DefaultRealm: realm,
			},
		},
		// TODO: Implement password-based authentication
	}, nil
}

// GetClient returns a Kerberos client for authentication
func (k *KerberosAuth) GetClient() (*client.Client, error) {
	if k.keytab == nil {
		return nil, fmt.Errorf("keytab not loaded")
	}

	// Load login credentials from keytab
	l := client.NewLoginWithKeytab(k.principal, k.realm)
	if l == nil {
		return nil, fmt.Errorf("failed to create login from keytab")
	}

	kt, err := keytab.Load(k.keytab)
	if err != nil {
		return nil, fmt.Errorf("failed to load keytab: %w", err)
	}

	err = l.Login(kt)
	if err != nil {
		return nil, fmt.Errorf("login failed: %w", err)
	}

	return l.Client, nil
}

// GetToken retrieves a Kerberos token for authentication
func (k *KerberosAuth) GetToken() (string, error) {
	cl, err := k.GetClient()
	if err != nil {
		return "", err
	}

	tkt, key, err := cl.GetServiceToken("HTTP")
	if err != nil {
		return "", fmt.Errorf("failed to get service ticket: %w", err)
	}

	// Convert token to GSSAPI format for transmission
	token, err := spnego.KRB5TokenFromKRB5Cred(tkt, key)
	if err != nil {
		return "", fmt.Errorf("failed to create SPNEGO token: %w", err)
	}

	return base64.StdEncoding.EncodeToString(token), nil
}

// RefreshToken refreshes an expired Kerberos token
func (k *KerberosAuth) RefreshToken() (string, error) {
	return k.GetToken()
}

// ValidateConfig validates the Kerberos configuration
func (k *KerberosAuth) ValidateConfig() error {
	if k.principal == "" {
		return fmt.Errorf("Kerberos principal is required")
	}
	if k.realm == "" {
		return fmt.Errorf("Kerberos realm is required")
	}
	return nil
}
