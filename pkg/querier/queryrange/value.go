// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/value.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/series"
)

// FromResult transforms a promql query result into a samplestream
func FromResult(res *promql.Result) ([]SampleStream, error) {
	if res.Err != nil {
		// The error could be wrapped by the PromQL engine. We get the error's cause in order to
		// correctly parse the error in parent callers (eg. gRPC response status code extraction).
		return nil, errors.Cause(res.Err)
	}
	switch v := res.Value.(type) {
	case promql.Scalar:
		return []SampleStream{
			{
				Samples: []mimirpb.Sample{
					{
						Value:       v.V,
						TimestampMs: v.T,
					},
				},
			},
		}, nil

	case promql.Vector:
		res := make([]SampleStream, 0, len(v))
		for _, sample := range v {
			res = append(res, SampleStream{
				Labels: mapLabels(sample.Metric),
				Samples: []mimirpb.Sample{{
					TimestampMs: sample.Point.T,
					Value:       sample.Point.V,
				}},
			})
		}
		return res, nil

	case promql.Matrix:
		res := make([]SampleStream, 0, len(v))
		for _, series := range v {
			res = append(res, SampleStream{
				Labels:  mapLabels(series.Metric),
				Samples: mimirpb.FromPointsToSamples(series.Points),
			})
		}
		return res, nil

	}

	return nil, errors.Errorf("Unexpected value type: [%s]", res.Value.Type())
}

func mapLabels(ls labels.Labels) []mimirpb.LabelAdapter {
	result := make([]mimirpb.LabelAdapter, 0, len(ls))
	for _, l := range ls {
		result = append(result, mimirpb.LabelAdapter(l))
	}

	return result
}

// ResponseToSamples is needed to map back from api response to the underlying series data
func ResponseToSamples(resp Response) ([]SampleStream, error) {
	promRes, ok := resp.(*PrometheusResponse)
	if !ok {
		return nil, errors.Errorf("error invalid response type: %T, expected: %T", resp, &PrometheusResponse{})
	}
	if promRes.Error != "" {
		return nil, errors.New(promRes.Error)
	}
	switch promRes.Data.ResultType {
	case string(parser.ValueTypeVector), string(parser.ValueTypeMatrix):
		return promRes.Data.Result, nil
	}

	return nil, errors.Errorf(
		"Invalid promql.Value type: [%s]. Only %s and %s supported",
		promRes.Data.ResultType,
		parser.ValueTypeVector,
		parser.ValueTypeMatrix,
	)
}

// NewSeriesSet returns an in memory storage.SeriesSet from a []SampleStream
// As NewSeriesSet uses NewConcreteSeriesSet to implement SeriesSet, result will be sorted by label names.
func NewSeriesSet(results []SampleStream) storage.SeriesSet {
	set := make([]storage.Series, 0, len(results))

	for _, stream := range results {
		samples := make([]model.SamplePair, 0, len(stream.Samples))
		for _, sample := range stream.Samples {
			samples = append(samples, model.SamplePair{
				Timestamp: model.Time(sample.TimestampMs),
				Value:     model.SampleValue(sample.Value),
			})
		}

		ls := make([]labels.Label, 0, len(stream.Labels))
		for _, l := range stream.Labels {
			ls = append(ls, labels.Label(l))
		}
		set = append(set, series.NewConcreteSeries(ls, samples))
	}
	return series.NewConcreteSeriesSet(set)
}
