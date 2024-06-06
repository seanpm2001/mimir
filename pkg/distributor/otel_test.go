// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

func BenchmarkOTLPHandler(b *testing.B) {
	var samples []prompb.Sample
	for i := 0; i < 1000; i++ {
		ts := time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Second)
		samples = append(samples, prompb.Sample{
			Value:     1,
			Timestamp: ts.UnixNano(),
		})
	}
	sampleSeries := []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "foo"},
			},
			Samples: samples,
			Histograms: []prompb.Histogram{
				remote.HistogramToHistogramProto(1337, test.GenerateTestHistogram(1)),
			},
		},
	}
	// Sample metadata needs to correspond to every series in the sampleSeries
	sampleMetadata := []mimirpb.MetricMetadata{
		{
			Help: "metric_help",
			Unit: "metric_unit",
		},
	}
	exportReq := TimeseriesToOTLPRequest(sampleSeries, sampleMetadata)

	pushFunc := func(_ context.Context, pushReq *Request) error {
		if _, err := pushReq.WriteRequest(); err != nil {
			return err
		}

		pushReq.CleanUp()
		return nil
	}
	limits, err := validation.NewOverrides(
		validation.Limits{},
		validation.NewMockTenantLimits(map[string]*validation.Limits{}),
	)
	require.NoError(b, err)
	handler := OTLPHandler(100000, nil, nil, true, limits, RetryConfig{}, pushFunc, nil, nil, log.NewNopLogger(), true)

	b.Run("protobuf", func(b *testing.B) {
		req := createOTLPProtoRequest(b, exportReq, false)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			require.Equal(b, http.StatusOK, resp.Code)
			req.Body.(*reusableReader).Reset()
		}
	})

	b.Run("JSON", func(b *testing.B) {
		req := createOTLPJSONRequest(b, exportReq, false)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			require.Equal(b, http.StatusOK, resp.Code)
			req.Body.(*reusableReader).Reset()
		}
	})
}

func createOTLPProtoRequest(tb testing.TB, metricRequest pmetricotlp.ExportRequest, compress bool) *http.Request {
	tb.Helper()

	body, err := metricRequest.MarshalProto()
	require.NoError(tb, err)

	return createOTLPRequest(tb, body, compress, "application/x-protobuf")
}

func createOTLPJSONRequest(tb testing.TB, metricRequest pmetricotlp.ExportRequest, compress bool) *http.Request {
	tb.Helper()

	body, err := metricRequest.MarshalJSON()
	require.NoError(tb, err)

	return createOTLPRequest(tb, body, compress, "application/json")
}

func createOTLPRequest(tb testing.TB, body []byte, compress bool, contentType string) *http.Request {
	tb.Helper()

	if compress {
		var b bytes.Buffer
		gz := gzip.NewWriter(&b)
		_, err := gz.Write(body)
		require.NoError(tb, err)
		require.NoError(tb, gz.Close())

		body = b.Bytes()
	}

	// reusableReader is suitable for benchmarks
	req, err := http.NewRequest("POST", "http://localhost/", newReusableReader(body))
	require.NoError(tb, err)
	// Since http.NewRequest will deduce content length only from known io.Reader implementations,
	// define it ourselves
	req.ContentLength = int64(len(body))
	req.Header.Set("Content-Type", contentType)
	const tenantID = "test"
	req.Header.Set("X-Scope-OrgID", tenantID)
	ctx := user.InjectOrgID(context.Background(), tenantID)
	req = req.WithContext(ctx)
	if compress {
		req.Header.Set("Content-Encoding", "gzip")
	}

	return req
}

type reusableReader struct {
	*bytes.Reader
	raw []byte
}

func newReusableReader(raw []byte) *reusableReader {
	return &reusableReader{
		Reader: bytes.NewReader(raw),
		raw:    raw,
	}
}

func (r *reusableReader) Close() error {
	return nil
}

func (r *reusableReader) Reset() {
	r.Reader.Reset(r.raw)
}

var _ io.ReadCloser = &reusableReader{}

func TestOtlpToTimeseriesWithBlessingAttributes(t *testing.T) {
	tenantID := "testTenant"
	discardedDueToOtelParseError := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "discarded_due_to_otel_parse_error",
		Help: "Number of metrics discarded due to OTLP parse errors.",
	}, []string{tenantID, "group"})

	// Helper function to create a pmetric.Metrics object
	createMetrics := func() pmetric.Metrics {
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("attr2", "value2")
		il := rm.ScopeMetrics().AppendEmpty()
		m := il.Metrics().AppendEmpty()
		m.SetName("test_metric")
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetIntValue(123)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		return md
	}
	testCases := []struct {
		name                        string
		expectedLabels              []mimirpb.LabelAdapter
		otelMetricsToTimeseriesFunc func(_ string, _ bool, _ []string, _ *prometheus.CounterVec, _ log.Logger, _ pmetric.Metrics) ([]mimirpb.PreallocTimeseries, error)
		promoteResourceAttributes   []string
	}{
		{
			name: "Successful conversion without blessing attributes new",
			expectedLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
			},
			otelMetricsToTimeseriesFunc: otelMetricsToTimeseries,
			promoteResourceAttributes:   []string{},
		},
		{
			name: "Successful conversion with blessing attributes new",
			expectedLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "attr2",
					Value: "value2",
				},
			},
			otelMetricsToTimeseriesFunc: otelMetricsToTimeseries,
			promoteResourceAttributes:   []string{"attr1", "attr2"},
		},
		{
			name: "Successful conversion without blessing attributes old",
			expectedLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
			},
			otelMetricsToTimeseriesFunc: otelMetricsToTimeseriesOld,
			promoteResourceAttributes:   []string{},
		},
		{
			name: "Successful conversion with blessing attributes old",
			expectedLabels: []mimirpb.LabelAdapter{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "attr2",
					Value: "value2",
				},
			},
			otelMetricsToTimeseriesFunc: otelMetricsToTimeseriesOld,
			promoteResourceAttributes:   []string{"attr1", "attr2"},
		},
	}

	md := createMetrics()
	for _, ts := range testCases {
		t.Run(ts.name, func(t *testing.T) {
			mimirTS, err := ts.otelMetricsToTimeseriesFunc(tenantID, true, ts.promoteResourceAttributes, discardedDueToOtelParseError, log.NewNopLogger(), md)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(mimirTS))
			assert.Equal(t, len(ts.expectedLabels), len(mimirTS[0].Labels))
			for i, l := range ts.expectedLabels {
				assert.Equal(t, 0, l.Compare(mimirTS[0].Labels[i]))
			}
		})
	}
}
