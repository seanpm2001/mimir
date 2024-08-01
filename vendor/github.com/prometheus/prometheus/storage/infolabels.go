// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"fmt"
	"math"

	"golang.org/x/exp/maps"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
)

type infoSeries struct {
	name              string
	identifyingLabels labels.Labels
	dataLabels        labels.Labels
	series            Series
}

// SeriesSetWithInfoLabels wraps a SeriesSet and enriches each of its series with info metric data labels.
type SeriesSetWithInfoLabels struct {
	Base  SeriesSet
	Hints *SelectHints
	Q     Querier

	inited     bool
	infoSeries []infoSeries
	buf        map[string]enrichedSeries
	cur        enrichedSeries
	err        error
}

// Next advances the iterator.
// The iterator advances by, for every advancement of its wrapped SeriesSet, enriching its current series
// with matching info metric data labels.
func (ss *SeriesSetWithInfoLabels) Next() bool {
	if ss.err != nil {
		return false
	}

	if !ss.inited {
		ss.init()
	}

	if len(ss.buf) > 0 {
		fmt.Printf("SeriesSetWithInfoLabels.Next: Consuming buf\n")
		k := maps.Keys(ss.buf)[0]
		ss.cur = ss.buf[k]
		delete(ss.buf, k)
		return true
	}

	// Advance to the next successfully enriched series.
	fmt.Printf("SeriesSetWithInfoLabels.Next: Calling Next on ss.Base %T\n", ss.Base)
	lb := labels.NewScratchBuilder(0)
	for ss.Base.Next() {
		ss.buf = map[string]enrichedSeries{}
		s := ss.Base.At()
		fmt.Printf("SeriesSetWithInfoLabels.Next: Getting info metric data labels for %s, from %T\n", s.Labels(), ss.Q)

		// Expand s into a set of series enriched with per-timestamp info metric data labels.
		sIt := s.Iterator(nil)
		for vt := sIt.Next(); vt != chunkenc.ValNone; vt = sIt.Next() {
			t := sIt.AtT()
			es := enrichedSeriesSample{
				t:  t,
				vt: vt,
			}
			switch vt {
			case chunkenc.ValFloat:
				_, es.f = sIt.At()
			case chunkenc.ValHistogram:
				_, es.h = sIt.AtHistogram(nil)
			case chunkenc.ValFloatHistogram:
				_, es.fh = sIt.AtFloatHistogram(nil)
			default:
				panic(fmt.Sprintf("unrecognized sample type %s", vt))
			}

			id, err := ss.enrichSeries(s, t, &lb)
			if err != nil {
				ss.err = err
				return false
			}
			if id == "" {
				continue
			}
			s := ss.buf[id]
			s.samples = append(s.samples, es)
			ss.buf[id] = s
		}

		k := maps.Keys(ss.buf)[0]
		ss.cur = ss.buf[k]
		delete(ss.buf, k)
		return true
	}

	return false
}

// init initializes ss and returns whether success or not.
func (ss *SeriesSetWithInfoLabels) init() bool {
	// Get matching info series.
	hints := &SelectHints{}
	if ss.Hints != nil {
		hints.Start = ss.Hints.Start
		hints.End = ss.Hints.End
		hints.Limit = ss.Hints.Limit
		hints.Step = ss.Hints.Step
		hints.ShardCount = ss.Hints.ShardCount
		hints.ShardIndex = ss.Hints.ShardIndex
		hints.DisableTrimming = ss.Hints.DisableTrimming
	}
	// NB: We only support target_info for now.
	// TODO: Pass regexp matchers for all of the base series' identifying label sets, in order to ignore irrelevant info series.
	// TODO: Pass data label matchers.
	infoIt := ss.Q.Select(context.TODO(), false, hints, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "target_info"))
	iLB := labels.NewScratchBuilder(0)
	dLB := labels.NewScratchBuilder(0)
	for infoIt.Next() {
		is := infoIt.At()
		var name string
		iLB.Reset()
		dLB.Reset()
		is.Labels().Range(func(l labels.Label) {
			switch l.Name {
			case "instance", "job":
				iLB.Add(l.Name, l.Value)
			case labels.MetricName:
				name = l.Value
			default:
				dLB.Add(l.Name, l.Value)
			}
		})
		iLB.Sort()
		dLB.Sort()
		ss.infoSeries = append(ss.infoSeries, infoSeries{
			name:              name,
			identifyingLabels: iLB.Labels(),
			dataLabels:        dLB.Labels(),
			series:            is,
		})
	}
	if infoIt.Err() != nil {
		ss.err = infoIt.Err()
		return false
	}

	ss.inited = true
	return true
}

// enrichSeries returns the correct info data label enriched series ID for timestamp t.
// If the corresponding one already exists in ss, its ID returned. Otherwise, a new one is created and added to ss before its ID gets returned.
func (ss *SeriesSetWithInfoLabels) enrichSeries(s Series, t int64, lb *labels.ScratchBuilder) (string, error) {
	lblMap := s.Labels().Map()
	isTimestamps := map[string]int64{}
	isLbls := map[string]labels.Labels{}
	for _, is := range ss.infoSeries {
		// Match identifying labels.
		isMatch := true
		is.identifyingLabels.Range(func(l labels.Label) {
			if sVal := lblMap[l.Name]; sVal != l.Value {
				isMatch = false
			}
		})
		if !isMatch {
			// This info series is not a match for s.
			continue
		}

		sIt := is.series.Iterator(nil)
		// Find the latest sample <= t.
		sT := int64(math.MinInt64)
		var sV float64
		for vt := sIt.Next(); vt != chunkenc.ValNone; vt = sIt.Next() {
			curT, curV := sIt.At()
			if curT == t {
				sT = curT
				sV = curV
				break
			}
			if curT > t {
				break
			}

			// < t.
			sT = curT
			sV = curV
		}
		if sIt.Err() != nil {
			return "", sIt.Err()
		}
		if sT < 0 || value.IsStaleNaN(sV) {
			continue
		}

		if existingTs, exists := isTimestamps[is.name]; exists && existingTs >= sT {
			// Newest one wins.
			continue
		}

		isTimestamps[is.name] = sT
		isLbls[is.name] = is.dataLabels
	}

	seen := s.Labels().Map()
	lb.Reset()
	for _, lbls := range isLbls {
		lbls.Range(func(l labels.Label) {
			if _, exists := seen[l.Name]; exists {
				return
			}
			seen[l.Name] = l.Value
			lb.Add(l.Name, l.Value)
		})
	}

	lb.Sort()
	infoLbls := lb.Labels()
	infoLblsStr := infoLbls.String()
	if _, exists := ss.buf[infoLblsStr]; exists {
		// fmt.Printf("Info labels already seen: %s\n", infoLblsStr)
		return infoLblsStr, nil
	}

	fmt.Printf("Creating enriched series for %s\n", infoLblsStr)
	lb.Assign(s.Labels())
	infoLbls.Range(func(l labels.Label) {
		lb.Add(l.Name, l.Value)
	})
	ss.buf[infoLblsStr] = enrichedSeries{
		labels: lb.Labels(),
	}
	return infoLblsStr, nil
}

func (ss *SeriesSetWithInfoLabels) At() Series {
	return ss.cur
}

func (ss *SeriesSetWithInfoLabels) Err() error {
	return ss.err
}

func (ss *SeriesSetWithInfoLabels) Warnings() annotations.Annotations {
	return nil
}

// enrichedSeries is a time series enriched with info metric data labels.
type enrichedSeries struct {
	labels  labels.Labels
	samples []enrichedSeriesSample
}

func (s enrichedSeries) Labels() labels.Labels {
	return s.labels
}

func (s enrichedSeries) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return &enrichedSeriesSampleIterator{
		samples: s.samples,
	}
}

type enrichedSeriesSample struct {
	t  int64
	vt chunkenc.ValueType
	f  float64
	h  *histogram.Histogram
	fh *histogram.FloatHistogram
}

type enrichedSeriesSampleIterator struct {
	samples []enrichedSeriesSample
	cur     enrichedSeriesSample
}

func (it *enrichedSeriesSampleIterator) Next() chunkenc.ValueType {
	if len(it.samples) == 0 {
		return chunkenc.ValNone
	}
	it.cur = it.samples[0]
	it.samples = it.samples[1:]
	return it.cur.vt
}

func (it *enrichedSeriesSampleIterator) AtT() int64 {
	return it.cur.t
}

func (it *enrichedSeriesSampleIterator) At() (int64, float64) {
	return it.cur.t, it.cur.f
}

func (it *enrichedSeriesSampleIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	return it.cur.t, it.cur.h
}

func (it *enrichedSeriesSampleIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return it.cur.t, it.cur.fh
}

func (it *enrichedSeriesSampleIterator) Err() error {
	return nil
}

func (it *enrichedSeriesSampleIterator) Warnings() annotations.Annotations {
	return nil
}

func (it *enrichedSeriesSampleIterator) Seek(t int64) chunkenc.ValueType {
	for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
		if it.AtT() >= t {
			return vt
		}
	}
	return chunkenc.ValNone
}
