// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package rabbitmqreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/rabbitmqreceiver"

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/rabbitmqreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/rabbitmqreceiver/internal/mocks"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/rabbitmqreceiver/internal/models"
)

func TestScraperStart(t *testing.T) {
	clientConfigNonexistentCA := confighttp.NewDefaultClientConfig()
	clientConfigNonexistentCA.Endpoint = defaultEndpoint
	clientConfigNonexistentCA.TLSSetting = configtls.ClientConfig{
		Config: configtls.Config{
			CAFile: "/non/existent",
		},
	}

	clientConfig := confighttp.NewDefaultClientConfig()
	clientConfig.Endpoint = defaultEndpoint

	testcases := []struct {
		desc        string
		scraper     *rabbitmqScraper
		expectError bool
	}{
		{
			desc: "Bad Config",
			scraper: &rabbitmqScraper{
				cfg: &Config{
					ClientConfig: clientConfigNonexistentCA,
				},
				settings: componenttest.NewNopTelemetrySettings(),
			},
			expectError: true,
		},
		{
			desc: "Valid Config",
			scraper: &rabbitmqScraper{
				cfg: &Config{
					ClientConfig: clientConfig,
				},
				settings: componenttest.NewNopTelemetrySettings(),
			},
			expectError: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			err := tc.scraper.start(context.Background(), componenttest.NewNopHost())
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestScraperScrape(t *testing.T) {
	testCases := []struct {
		desc              string
		setupMockClient   func(t *testing.T) client
		expectedMetricGen func(t *testing.T) pmetric.Metrics
		expectedErr       error
	}{
		{
			desc: "Nil client",
			setupMockClient: func(*testing.T) client {
				return nil
			},
			expectedMetricGen: func(*testing.T) pmetric.Metrics {
				return pmetric.NewMetrics()
			},
			expectedErr: errClientNotInit,
		},
		{
			desc: "API Call Failure",
			setupMockClient: func(*testing.T) client {
				mockClient := mocks.MockClient{}
				mockClient.On("GetQueues", mock.Anything).Return(nil, errors.New("some api error"))
				mockClient.On("GetNodes", mock.Anything).Return(nil, errors.New("some api error"))
				return &mockClient
			},
			expectedMetricGen: func(*testing.T) pmetric.Metrics {
				return pmetric.NewMetrics()
			},
			expectedErr: scrapererror.NewPartialScrapeError(
				errors.New("failed to collect queue metrics: some api error; failed to collect node metrics: some api error"),
				0, // No metrics were collected
			),
		},
		{
			desc: "Successful Queue Collection",
			setupMockClient: func(t *testing.T) client {
				mockClient := mocks.MockClient{}
				// use helper function from client tests
				data := loadAPIResponseData(t, queuesAPIResponseFile)
				var queues []*models.Queue
				err := json.Unmarshal(data, &queues)
				require.NoError(t, err)

				mockClient.On("GetQueues", mock.Anything).Return(queues, nil)
				mockClient.On("GetNodes", mock.Anything).Return(nil, nil)
				return &mockClient
			},
			expectedMetricGen: func(t *testing.T) pmetric.Metrics {
				goldenPath := filepath.Join("testdata", "expected_metrics", "metrics_golden.yaml")
				expectedMetrics, err := golden.ReadMetrics(goldenPath)
				require.NoError(t, err)
				return expectedMetrics
			},
			expectedErr: nil,
		},
		{
			desc: "Successful Queue + Node Metrics Collection",
			setupMockClient: func(t *testing.T) client {
				mockClient := mocks.MockClient{}

				// Fixed: relative path only
				queueData := loadAPIResponseData(t, queuesAPIResponseFile)
				var queues []*models.Queue
				err := json.Unmarshal(queueData, &queues)
				require.NoError(t, err)

				nodeData := loadAPIResponseData(t, nodesAPIResponseFile)

				var nodes []*models.Node
				err = json.Unmarshal(nodeData, &nodes)
				require.NoError(t, err)

				require.NotEmpty(t, nodes, "Mock node list should not be empty")

				mockClient.On("GetQueues", mock.Anything).Return(queues, nil)
				mockClient.On("GetNodes", mock.Anything).Return(nodes, nil)

				return &mockClient
			},
			expectedMetricGen: func(t *testing.T) pmetric.Metrics {
				goldenPath := filepath.Join("testdata", "expected_metrics", "metrics_golden_queues_nodes.yaml")
				expectedMetrics, err := golden.ReadMetrics(goldenPath)
				require.NoError(t, err)
				return expectedMetrics
			},
			expectedErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)

			// Enable all 17 node metrics
			cfg.MetricsBuilderConfig.Metrics.RabbitmqNodeDiskFree.Enabled = true
			cfg.MetricsBuilderConfig.Metrics.RabbitmqNodeDiskFreeLimit.Enabled = true
			cfg.MetricsBuilderConfig.Metrics.RabbitmqNodeDiskFreeAlarm.Enabled = true
			cfg.MetricsBuilderConfig.Metrics.RabbitmqNodeDiskFreeDetailsRate.Enabled = true

			cfg.MetricsBuilderConfig.Metrics.RabbitmqNodeMemUsed.Enabled = true
			cfg.MetricsBuilderConfig.Metrics.RabbitmqNodeMemLimit.Enabled = true
			cfg.MetricsBuilderConfig.Metrics.RabbitmqNodeMemAlarm.Enabled = true
			cfg.MetricsBuilderConfig.Metrics.RabbitmqNodeMemUsedDetailsRate.Enabled = true

			cfg.MetricsBuilderConfig.Metrics.RabbitmqNodeFdUsed.Enabled = true
			cfg.MetricsBuilderConfig.Metrics.RabbitmqNodeFdTotal.Enabled = true
			cfg.MetricsBuilderConfig.Metrics.RabbitmqNodeFdUsedDetailsRate.Enabled = true

			cfg.MetricsBuilderConfig.Metrics.RabbitmqNodeSocketsUsed.Enabled = true
			cfg.MetricsBuilderConfig.Metrics.RabbitmqNodeSocketsTotal.Enabled = true
			cfg.MetricsBuilderConfig.Metrics.RabbitmqNodeSocketsUsedDetailsRate.Enabled = true

			cfg.MetricsBuilderConfig.Metrics.RabbitmqNodeProcUsed.Enabled = true
			cfg.MetricsBuilderConfig.Metrics.RabbitmqNodeProcTotal.Enabled = true
			cfg.MetricsBuilderConfig.Metrics.RabbitmqNodeProcUsedDetailsRate.Enabled = true

			scraper := newScraper(zap.NewNop(), cfg, receivertest.NewNopSettings(metadata.Type))
			scraper.client = tc.setupMockClient(t)

			actualMetrics, err := scraper.scrape(context.Background())
			if tc.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedErr.Error())
			}

			expectedMetrics := tc.expectedMetricGen(t)

			require.NoError(t, pmetrictest.CompareMetrics(expectedMetrics, actualMetrics,
				pmetrictest.IgnoreStartTimestamp(), pmetrictest.IgnoreTimestamp(),
				pmetrictest.IgnoreResourceMetricsOrder(),
				pmetrictest.IgnoreMetricDataPointsOrder(),
				pmetrictest.IgnoreMetricsOrder(),
			))
		})
	}
}
