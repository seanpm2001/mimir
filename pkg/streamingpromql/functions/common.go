// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// SeriesMetadataFunction is a function to operate on the metadata across series.
type SeriesMetadataFunction func(seriesMetadata []types.SeriesMetadata, pool *pooling.LimitingPool) ([]types.SeriesMetadata, error)

func DropSeriesName(seriesMetadata []types.SeriesMetadata, _ *pooling.LimitingPool) ([]types.SeriesMetadata, error) {
	for i := range seriesMetadata {
		seriesMetadata[i].Labels = seriesMetadata[i].Labels.DropMetricName()
	}

	return seriesMetadata, nil
}

// InstantVectorFunction is a function that takes in a instant vector and produces an instant vector.
type InstantVectorFunction func(seriesData types.InstantVectorSeriesData, pool *pooling.LimitingPool) (types.InstantVectorSeriesData, error)

// floatTransformationFunc is not needed elsewhere, so it is not exported yet
func floatTransformationFunc(transform func(f float64) float64) InstantVectorFunction {
	return func(seriesData types.InstantVectorSeriesData, _ *pooling.LimitingPool) (types.InstantVectorSeriesData, error) {
		for i := range seriesData.Floats {
			seriesData.Floats[i].F = transform(seriesData.Floats[i].F)
		}
		return seriesData, nil
	}
}

func FloatTransformationDropHistogramsFunc(transform func(f float64) float64) InstantVectorFunction {
	ft := floatTransformationFunc(transform)
	return func(seriesData types.InstantVectorSeriesData, pool *pooling.LimitingPool) (types.InstantVectorSeriesData, error) {
		// Functions that do not explicitly mention native histograms in their documentation will ignore histogram samples.
		// https://prometheus.io/docs/prometheus/latest/querying/functions
		pool.PutHPointSlice(seriesData.Histograms)
		seriesData.Histograms = nil
		return ft(seriesData, pool)
	}
}

func Passthrough(seriesData types.InstantVectorSeriesData, _ *pooling.LimitingPool) (types.InstantVectorSeriesData, error) {
	return seriesData, nil
}

// RangeVectorStepFunction is a function that operates on a range vector step.
//
// floatBuffer and histogramBuffer will contain the values for the step range.
// They may also contain values outside of the step, so step.RangeEnd needs to be considered.
//
// Parameters:
//   - step: the range vector step data to be processed.
//   - rangeSeconds: the duration of the range in seconds.
//   - floatBuffer: a ring buffer containing float values for the step range.
//   - histogramBuffer: a ring buffer containing histogram values for the step range.
//   - emitAnnotation: a callback function to emit an annotation for the current series.
//
// Returns:
//   - hasFloat bool: a boolean indicating if a float value is present.
//   - f float64: the float value.
//   - h *histogram.FloatHistogram: nil if no histogram is present.
//   - err error.
type RangeVectorStepFunction func(
	step types.RangeVectorStepData,
	rangeSeconds float64,
	floatBuffer *types.FPointRingBuffer,
	histogramBuffer *types.HPointRingBuffer,
	emitAnnotation EmitAnnotationFunc,
) (f float64, hasFloat bool, h *histogram.FloatHistogram, err error)

// EmitAnnotationFunc is a function that emits the annotation created by generator.
type EmitAnnotationFunc func(generator AnnotationGenerator)

// AnnotationGenerator is a function that returns an annotation for the given metric name and expression position.
type AnnotationGenerator func(metricName string, expressionPosition posrange.PositionRange) error

// RangeVectorSeriesValidationFunction is a function that is called after a series is completed for a function over a range vector.
type RangeVectorSeriesValidationFunction func(seriesData types.InstantVectorSeriesData, metricName string, emitAnnotation EmitAnnotationFunc)

type FunctionOverRangeVector struct {
	// StepFunc is the function that computes an output sample for a single step.
	StepFunc RangeVectorStepFunction

	// SeriesValidationFunc is the function that validates a complete series, emitting any annotations for that series.
	//
	// SeriesValidationFunc can be nil, in which case no validation is performed.
	SeriesValidationFunc RangeVectorSeriesValidationFunction

	// SeriesMetadataFunc is the function that computes the output series for this function based on the given input series.
	SeriesMetadataFunc SeriesMetadataFunction

	// NeedsSeriesNamesForAnnotations indicates that this function uses the names of input series when emitting annotations.
	//
	// NeedsSeriesNamesForAnnotations is implied if SeriesValidationFunc is non-nil.
	NeedsSeriesNamesForAnnotations bool
}
