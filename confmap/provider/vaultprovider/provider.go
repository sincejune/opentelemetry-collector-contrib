// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package vaultprovider // import "github.com/open-telemetry/opentelemetry-collector-contrib/confmap/provider/vaultprovider"
import (
	"context"
	"errors"
	"github.com/hashicorp/vault/api"
	"go.opentelemetry.io/collector/confmap"
	"strings"
)

const (
	schemeName = "vault"
)

var (
	emptyUriError      = errors.New("empty URI")
	invalidSchemeError = errors.New("invalid scheme")
)

func NewFactory() confmap.ProviderFactory {
	return confmap.NewProviderFactory(newWithSettings)
}

func newWithSettings(s confmap.ProviderSettings) confmap.Provider {
	return &provider{
		client: nil,
	}
}

type provider struct {
	client *api.Client
}

func (p provider) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (*confmap.Retrieved, error) {
	if p.client == nil {
		config := api.DefaultConfig()
		err := config.ReadEnvironment()
		if err != nil {
			return nil, err
		}
		client, err := api.NewClient(config)
		if err != nil {
			return nil, err
		}
		p.client = client
	}
	path, key, err := extractKey(uri)
	if err != nil {
		return nil, err
	}
	secret, err := p.client.Logical().Read(path)
	if err != nil {
		return nil, err
	}
	value := secret.Data["data"].(map[string]interface{})[key]
	return confmap.NewRetrieved(value)
}

func (p provider) Scheme() string {
	return schemeName
}

func (p provider) Shutdown(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func extractKey(uri string) (string, string, error) {
	if uri == "" {
		return "", "", emptyUriError
	}
	if !strings.HasPrefix(uri, schemeName+":") {
		return "", "", invalidSchemeError
	}
	withoutScheme := uri[len(schemeName+":"):]
	indexOfKeyPart := strings.LastIndex(withoutScheme, ":")

	return withoutScheme[0:indexOfKeyPart], withoutScheme[indexOfKeyPart+1:], nil
}
