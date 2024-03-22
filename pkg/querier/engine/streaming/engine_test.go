// SPDX-License-Identifier: AGPL-3.0-only

package streaming

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestUnsupportedPromQLFeatures(t *testing.T) {
	db := newTestDB(t)
	opts := newTestEngineOpts()
	engine, err := NewEngine(opts)
	require.NoError(t, err)
	ctx := context.Background()

	// The goal of this is not to list every conceivable expression that is unsupported, but to cover all the
	// different cases and make sure we produce a reasonable error message when these cases are encountered.
	unsupportedExpressions := map[string]string{
		"a + b":                        "PromQL expression type *parser.BinaryExpr",
		"1 + 2":                        "PromQL expression type *parser.BinaryExpr",
		"metric{} + other_metric{}":    "PromQL expression type *parser.BinaryExpr",
		"1":                            "PromQL expression type *parser.NumberLiteral",
		"metric{} offset 2h":           "instant vector selector with 'offset'",
		"metric{} @ 123":               "instant vector selector with '@' modifier",
		"metric{} @ start()":           "instant vector selector with '@ start()'",
		"metric{} @ end()":             "instant vector selector with '@ end()'",
		"avg(metric{})":                "'avg' aggregation",
		"sum without(l) (metric{})":    "grouping with 'without'",
		"rate(metric{}[5m] offset 2h)": "range vector selector with 'offset'",
		"rate(metric{}[5m] @ 123)":     "range vector selector with '@' modifier",
		"rate(metric{}[5m] @ start())": "range vector selector with '@ start()'",
		"rate(metric{}[5m] @ end())":   "range vector selector with '@ end()'",
		"avg_over_time(metric{}[5m])":  "'avg_over_time' function",
		"-sum(metric{})":               "PromQL expression type *parser.UnaryExpr",
		"(metric{})":                   "PromQL expression type *parser.ParenExpr",
	}

	for expression, expectedError := range unsupportedExpressions {
		t.Run(expression, func(t *testing.T) {
			qry, err := engine.NewRangeQuery(ctx, db, nil, expression, time.Now().Add(-time.Hour), time.Now(), time.Minute)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrNotSupported)
			require.EqualError(t, err, "not supported by streaming engine: "+expectedError)
			require.Nil(t, qry)

			qry, err = engine.NewInstantQuery(ctx, db, nil, expression, time.Now())
			require.Error(t, err)
			require.ErrorIs(t, err, ErrNotSupported)
			require.EqualError(t, err, "not supported by streaming engine: "+expectedError)
			require.Nil(t, qry)
		})
	}

	// These expressions are also unsupported, but are only valid as instant queries.
	unsupportedInstantQueryExpressions := map[string]string{
		"'a'":                    "PromQL expression type *parser.StringLiteral",
		"metric{}[5m]":           "PromQL expression type *parser.MatrixSelector",
		"metric{}[5m] offset 2h": "PromQL expression type *parser.MatrixSelector",
		"metric{}[5m] @ 123":     "PromQL expression type *parser.MatrixSelector",
		"metric{}[5m] @ start()": "PromQL expression type *parser.MatrixSelector",
		"metric{}[5m] @ end()":   "PromQL expression type *parser.MatrixSelector",
		"metric{}[5m:1m]":        "PromQL expression type *parser.SubqueryExpr",
	}

	for expression, expectedError := range unsupportedInstantQueryExpressions {
		t.Run(expression, func(t *testing.T) {
			qry, err := engine.NewInstantQuery(ctx, db, nil, expression, time.Now())
			require.Error(t, err)
			require.ErrorIs(t, err, ErrNotSupported)
			require.EqualError(t, err, "not supported by streaming engine: "+expectedError)
			require.Nil(t, qry)
		})
	}
}

func TestNewRangeQuery_InvalidQueryTime(t *testing.T) {
	opts := newTestEngineOpts()
	engine, err := NewEngine(opts)
	require.NoError(t, err)
	ctx := context.Background()

	_, err = engine.NewRangeQuery(ctx, nil, nil, "vector(0)", time.Now(), time.Now(), 0)
	require.EqualError(t, err, "0s is not a valid interval for a range query, must be greater than 0")

	start := time.Date(2024, 3, 22, 3, 0, 0, 0, time.UTC)
	_, err = engine.NewRangeQuery(ctx, nil, nil, "vector(0)", start, start.Add(-time.Hour), time.Second)
	require.EqualError(t, err, "range query time range is invalid: end time 2024-03-22T02:00:00Z is before start time 2024-03-22T03:00:00Z")
}

func TestNewRangeQuery_InvalidExpressionTypes(t *testing.T) {
	opts := newTestEngineOpts()
	engine, err := NewEngine(opts)
	require.NoError(t, err)
	ctx := context.Background()

	_, err = engine.NewRangeQuery(ctx, nil, nil, "metric[3m]", time.Now(), time.Now(), time.Second)
	require.EqualError(t, err, "query expression produces a range vector, but expression for range queries must produce an instant vector or scalar")

	_, err = engine.NewRangeQuery(ctx, nil, nil, `"thing"`, time.Now(), time.Now(), time.Second)
	require.EqualError(t, err, "query expression produces a string, but expression for range queries must produce an instant vector or scalar")
}

func newTestEngineOpts() promql.EngineOpts {
	return promql.EngineOpts{
		Logger:               nil,
		Reg:                  nil,
		MaxSamples:           50000000,
		Timeout:              100 * time.Second,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	}
}
