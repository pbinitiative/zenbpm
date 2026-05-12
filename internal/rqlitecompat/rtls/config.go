package rtls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

func CreateClientConfig(certFile, keyFile, caCertFile, serverName string, noverify bool) (*tls.Config, error) {
	cfg := &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: noverify,
		NextProtos:         []string{"h2", "http/1.1"},
		MinVersion:         tls.VersionTLS12,
	}
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	if caCertFile != "" {
		pem, err := os.ReadFile(caCertFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("failed to load CA certificate(s) from %q", caCertFile)
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}
