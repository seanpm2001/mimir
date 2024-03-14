// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/bench_test.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/util/teststorage/storage.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streaming

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

type benchCase struct {
	expr  string
	steps int
}

// These test cases are taken from https://github.com/prometheus/prometheus/blob/main/promql/bench_test.go.
func testCases(metricSizes []int) []benchCase {
	cases := []benchCase{
		// Plain retrieval.
		{
			expr: "a_X",
		},
		// Simple rate.
		{
			expr: "rate(a_X[1m])",
		},
		{
			expr:  "rate(a_X[1m])",
			steps: 10000,
		},
		//// Holt-Winters and long ranges.
		//{
		//	expr: "holt_winters(a_X[1d], 0.3, 0.3)",
		//},
		//{
		//	expr: "changes(a_X[1d])",
		//},
		{
			expr: "rate(a_X[1d])",
		},
		//{
		//	expr: "absent_over_time(a_X[1d])",
		//},
		//// Unary operators.
		//{
		//	expr: "-a_X",
		//},
		//// Binary operators.
		//{
		//	expr: "a_X - b_X",
		//},
		//{
		//	expr:  "a_X - b_X",
		//	steps: 10000,
		//},
		//{
		//	expr: "a_X and b_X{l=~'.*[0-4]$'}",
		//},
		//{
		//	expr: "a_X or b_X{l=~'.*[0-4]$'}",
		//},
		//{
		//	expr: "a_X unless b_X{l=~'.*[0-4]$'}",
		//},
		//{
		//	expr: "a_X and b_X{l='notfound'}",
		//},
		//// Simple functions.
		//{
		//	expr: "abs(a_X)",
		//},
		//{
		//	expr: "label_replace(a_X, 'l2', '$1', 'l', '(.*)')",
		//},
		//{
		//	expr: "label_join(a_X, 'l2', '-', 'l', 'l')",
		//},
		// Simple aggregations.
		{
			expr: "sum(a_X)",
		},
		//{
		//	expr: "sum without (l)(h_X)",
		//},
		//{
		//	expr: "sum without (le)(h_X)",
		//},
		{
			expr: "sum by (l)(h_X)",
		},
		{
			expr: "sum by (le)(h_X)",
		},
		//{
		//	expr: "count_values('value', h_X)",
		//  steps: 100,
		//},
		//{
		//	expr: "topk(1, a_X)",
		//},
		//{
		//	expr: "topk(5, a_X)",
		//},
		//// Combinations.
		//{
		//	expr: "rate(a_X[1m]) + rate(b_X[1m])",
		//},
		{
			expr: "sum by (le)(rate(h_X[1m]))",
		},
		//{
		//	expr: "sum without (l)(rate(a_X[1m]))",
		//},
		//{
		//	expr: "sum without (l)(rate(a_X[1m])) / sum without (l)(rate(b_X[1m]))",
		//},
		//{
		//	expr: "histogram_quantile(0.9, rate(h_X[5m]))",
		//},
		//// Many-to-one join.
		//{
		//	expr: "a_X + on(l) group_right a_one",
		//},
		//// Label compared to blank string.
		//{
		//	expr:  "count({__name__!=\"\"})",
		//	steps: 1,
		//},
		//{
		//	expr:  "count({__name__!=\"\",l=\"\"})",
		//	steps: 1,
		//},
		//// Functions which have special handling inside eval()
		//{
		//	expr: "timestamp(a_X)",
		//},
	}

	// X in an expr will be replaced by different metric sizes.
	tmp := []benchCase{}
	for _, c := range cases {
		if !strings.Contains(c.expr, "X") {
			tmp = append(tmp, c)
		} else {
			for _, count := range metricSizes {
				tmp = append(tmp, benchCase{expr: strings.ReplaceAll(c.expr, "X", strconv.Itoa(count)), steps: c.steps})
			}
		}
	}
	cases = tmp

	// No step will be replaced by cases with the standard step.
	tmp = []benchCase{}
	for _, c := range cases {
		if c.steps != 0 {
			tmp = append(tmp, c)
		} else {
			tmp = append(tmp, benchCase{expr: c.expr, steps: 0})
			tmp = append(tmp, benchCase{expr: c.expr, steps: 1})
			tmp = append(tmp, benchCase{expr: c.expr, steps: 100})
			tmp = append(tmp, benchCase{expr: c.expr, steps: 1000})
		}
	}
	return tmp
}

// This is based on the benchmarks from https://github.com/prometheus/prometheus/blob/main/promql/bench_test.go.
func BenchmarkQuery(b *testing.B) {
	db := newTestDB(b)
	db.DisableCompactions() // Don't want auto-compaction disrupting timings.
	opts := promql.EngineOpts{
		Logger:               nil,
		Reg:                  nil,
		MaxSamples:           50000000,
		Timeout:              100 * time.Second,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	}

	streamingEngine, err := NewEngine(opts)
	require.NoError(b, err)

	engines := map[string]promql.QueryEngine{
		"standard":  promql.NewEngine(opts),
		"streaming": streamingEngine,
	}

	const interval = 10000 // 10s interval.
	// A day of data plus 10k steps.
	numIntervals := 8640 + 10000

	metricSizes := []int{1, 10, 100, 2000}
	err = setupTestData(db, metricSizes, interval, numIntervals)
	require.NoError(b, err)
	cases := testCases(metricSizes)

	for _, c := range cases {
		b.Run(fmt.Sprintf("expr=%s,steps=%d", c.expr, c.steps), func(b *testing.B) {
			for name, engine := range engines {
				b.Run(name, func(b *testing.B) {
					ctx := context.Background()
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						var qry promql.Query
						var err error

						start := time.Unix(int64((numIntervals-c.steps)*10), 0)
						end := time.Unix(int64(numIntervals*10), 0)
						interval := time.Second * 10

						if c.steps == 0 {
							qry, err = engine.NewInstantQuery(ctx, db, nil, c.expr, start)
						} else {
							qry, err = engine.NewRangeQuery(ctx, db, nil, c.expr, start, end, interval)
						}

						if err != nil {
							require.NoError(b, err)
						}
						res := qry.Exec(ctx)
						if res.Err != nil {
							require.NoError(b, res.Err)
						}
						qry.Close()
					}
				})
			}
		})
	}
}

func TestQuery(t *testing.T) {
	db := newTestDB(t)
	opts := promql.EngineOpts{
		Logger:               nil,
		Reg:                  nil,
		MaxSamples:           50000000,
		Timeout:              100 * time.Second,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	}

	standardEngine := promql.NewEngine(opts)
	streamingEngine, err := NewEngine(opts)
	require.NoError(t, err)

	const interval = 10000 // 10s interval.
	// A day of data plus 10k steps.
	numIntervals := 8640 + 10000

	metricSizes := []int{1, 10, 100}
	err = setupTestData(db, metricSizes, interval, numIntervals)
	require.NoError(t, err)
	cases := testCases(metricSizes)

	runQuery := func(t *testing.T, engine promql.QueryEngine, c benchCase) (*promql.Result, func()) {
		ctx := context.Background()
		var qry promql.Query
		var err error

		start := time.Unix(int64((numIntervals-c.steps)*10), 0)
		end := time.Unix(int64(numIntervals*10), 0)
		interval := time.Second * 10

		if c.steps == 0 {
			qry, err = engine.NewInstantQuery(ctx, db, nil, c.expr, start)
		} else {
			qry, err = engine.NewRangeQuery(ctx, db, nil, c.expr, start, end, interval)
		}

		if errors.Is(err, ErrNotSupported) {
			t.Skipf("skipping, query is not supported by streaming engine: %v", err)
		}

		require.NoError(t, err)
		res := qry.Exec(ctx)
		require.NoError(t, res.Err)

		return res, qry.Close
	}

	for _, c := range cases {
		name := fmt.Sprintf("expr=%s,steps=%d", c.expr, c.steps)
		t.Run(name, func(t *testing.T) {
			standardResult, standardClose := runQuery(t, standardEngine, c)
			streamingResult, streamingClose := runQuery(t, streamingEngine, c)

			// Why do we do this rather than require.Equal(t, standardResult, streamingResult)?
			// It's possible that floating point values are slightly different due to imprecision, but require.Equal doesn't allow us to set an allowable difference.
			require.Equal(t, standardResult.Err, streamingResult.Err)

			// Ignore warnings until they're supported by the streaming engine.
			// require.Equal(t, standardResult.Warnings, streamingResult.Warnings)

			if c.steps == 0 {
				standardVector, err := standardResult.Vector()
				require.NoError(t, err)
				streamingVector, err := streamingResult.Vector()
				require.NoError(t, err)

				// There's no guarantee that series from the standard engine are sorted.
				slices.SortFunc(standardVector, func(a, b promql.Sample) int {
					return labels.Compare(a.Metric, b.Metric)
				})

				require.Len(t, streamingVector, len(standardVector))

				for i, standardSample := range standardVector {
					streamingSample := streamingVector[i]

					require.Equal(t, standardSample.Metric, streamingSample.Metric)
					require.Equal(t, standardSample.T, streamingSample.T)
					require.Equal(t, standardSample.H, streamingSample.H)
					require.InEpsilon(t, standardSample.F, streamingSample.F, 1e-10)
				}
			} else {
				standardMatrix, err := standardResult.Matrix()
				require.NoError(t, err)
				streamingMatrix, err := streamingResult.Matrix()
				require.NoError(t, err)

				// There's no guarantee that series from the standard engine are sorted.
				slices.SortFunc(standardMatrix, func(a, b promql.Series) int {
					return labels.Compare(a.Metric, b.Metric)
				})

				require.Len(t, streamingMatrix, len(standardMatrix))

				for i, standardSeries := range standardMatrix {
					streamingSeries := streamingMatrix[i]

					require.Equal(t, standardSeries.Metric, streamingSeries.Metric)
					require.Equal(t, standardSeries.Histograms, streamingSeries.Histograms)

					for j, standardPoint := range standardSeries.Floats {
						streamingPoint := streamingSeries.Floats[j]

						require.Equal(t, standardPoint.T, streamingPoint.T)
						require.InEpsilonf(t, standardPoint.F, streamingPoint.F, 1e-10, "expected series %v to have points %v, but streaming result is %v", standardSeries.Metric.String(), standardSeries.Floats, streamingSeries.Floats)
					}
				}
			}

			standardClose()
			streamingClose()
		})
	}
}

func setupTestData(db *tsdb.DB, metricSizes []int, interval, numIntervals int) error {
	totalMetrics := 0

	for _, size := range metricSizes {
		totalMetrics += 13 * size // 2 non-histogram metrics + 11 metrics for histogram buckets
	}

	metrics := make([]labels.Labels, 0, totalMetrics)

	for _, size := range metricSizes {
		aName := "a_" + strconv.Itoa(size)
		bName := "b_" + strconv.Itoa(size)
		histogramName := "h_" + strconv.Itoa(size)

		if size == 1 {
			// We don't want a "l" label on metrics with one series (some test cases rely on this label not being present).
			metrics = append(metrics, labels.FromStrings("__name__", aName))
			metrics = append(metrics, labels.FromStrings("__name__", bName))
			for le := 0; le < 10; le++ {
				metrics = append(metrics, labels.FromStrings("__name__", histogramName, "le", strconv.Itoa(le)))
			}
			metrics = append(metrics, labels.FromStrings("__name__", histogramName, "le", "+Inf"))
		} else {
			for i := 0; i < size; i++ {
				metrics = append(metrics, labels.FromStrings("__name__", aName, "l", strconv.Itoa(i)))
				metrics = append(metrics, labels.FromStrings("__name__", bName, "l", strconv.Itoa(i)))
				for le := 0; le < 10; le++ {
					metrics = append(metrics, labels.FromStrings("__name__", histogramName, "l", strconv.Itoa(i), "le", strconv.Itoa(le)))
				}
				metrics = append(metrics, labels.FromStrings("__name__", histogramName, "l", strconv.Itoa(i), "le", "+Inf"))
			}
		}
	}

	refs := make([]storage.SeriesRef, len(metrics))

	for s := 0; s < numIntervals; s++ {
		a := db.Appender(context.Background())
		ts := int64(s * interval)
		for i, metric := range metrics {
			ref, _ := a.Append(refs[i], metric, ts, float64(s)+float64(i)/float64(len(metrics)))
			refs[i] = ref
		}
		if err := a.Commit(); err != nil {
			return err
		}
	}

	db.ForceHeadMMap() // Ensure we have at most one head chunk for every series.
	return db.Compact(context.Background())
}

// This is based on https://github.com/prometheus/prometheus/blob/main/util/teststorage/storage.go, but with isolation disabled
// to improve test setup performance and mirror Mimir's default configuration.
func newTestDB(t testing.TB) *tsdb.DB {
	dir := t.TempDir()

	// Tests just load data for a series sequentially. Thus we need a long appendable window.
	opts := tsdb.DefaultOptions()
	opts.MinBlockDuration = int64(24 * time.Hour / time.Millisecond)
	opts.MaxBlockDuration = int64(24 * time.Hour / time.Millisecond)
	opts.RetentionDuration = 0
	opts.EnableNativeHistograms = true
	opts.IsolationDisabled = true
	db, err := tsdb.Open(dir, nil, nil, opts, tsdb.NewDBStats())
	require.NoError(t, err, "unexpected error while opening test storage")

	t.Cleanup(func() {
		require.NoError(t, db.Close(), "unexpected error while closing test storage")
	})

	return db
}