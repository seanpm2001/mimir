// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type Pusher interface {
	PushToStorage(context.Context, *mimirpb.WriteRequest) error
}

type pusherConsumerPrototype struct {
	processingTimeSeconds prometheus.Observer
	clientErrRequests     prometheus.Counter
	serverErrRequests     prometheus.Counter
	totalRequests         prometheus.Counter

	fallbackClientErrSampler *util_log.Sampler // Fallback log message sampler client errors that are not sampled yet.
	logger                   log.Logger
}

type parsedRecord struct {
	*mimirpb.WriteRequest
	// Context holds the tracing and cancellation data for this record/request.
	ctx      context.Context
	tenantID string
	err      error
}

type PusherCloser interface {
	Pusher
	Close() []error
}

type pusherConsumer struct {
	pusherConsumerPrototype
	pusher PusherCloser
}

func (c pusherConsumer) Close(ctx context.Context) []error {
	spanLog := spanlogger.FromContext(ctx, log.NewNopLogger())
	errs := c.pusher.Close()
	for eIdx := 0; eIdx < len(errs); eIdx++ {
		err := errs[eIdx]
		isServerErr := c.handlePushErr(ctx, "TODO", err, spanLog)
		if !isServerErr {
			errs[len(errs)-1], errs[eIdx] = errs[eIdx], errs[len(errs)-1]
			errs = errs[:len(errs)-1]
			eIdx--
		}
	}
	return errs
}

func newPusherConsumer(p PusherCloser, proto pusherConsumerPrototype) *pusherConsumer {
	return &pusherConsumer{
		pusher:                  p,
		pusherConsumerPrototype: proto,
	}
}

func newPusherConsumerPrototype(fallbackClientErrSampler *util_log.Sampler, reg prometheus.Registerer, l log.Logger) pusherConsumerPrototype {
	errRequestsCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingest_storage_reader_records_failed_total",
		Help: "Number of records (write requests) which caused errors while processing. Client errors are errors such as tenant limits and samples out of bounds. Server errors indicate internal recoverable errors.",
	}, []string{"cause"})

	return pusherConsumerPrototype{
		logger:                   l,
		fallbackClientErrSampler: fallbackClientErrSampler,
		processingTimeSeconds: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "cortex_ingest_storage_reader_processing_time_seconds",
			Help:                            "Time taken to process a single record (write request).",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}),
		clientErrRequests: errRequestsCounter.WithLabelValues("client"),
		serverErrRequests: errRequestsCounter.WithLabelValues("server"),
		totalRequests: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_records_total",
			Help: "Number of attempted records (write requests).",
		}),
	}
}

func (c pusherConsumer) consume(ctx context.Context, records []record) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(cancellation.NewErrorf("done consuming records"))

	recC := make(chan parsedRecord)

	// Speed up consumption by unmarhsalling the next request while the previous one is being pushed.
	go c.unmarshalRequests(ctx, records, recC)
	return c.pushRequests(recC)
}

func (c pusherConsumer) pushRequests(reqC <-chan parsedRecord) error {
	recordIdx := -1
	for wr := range reqC {
		recordIdx++
		if wr.err != nil {
			level.Error(c.logger).Log("msg", "failed to parse write request; skipping", "err", wr.err)
			continue
		}

		err := c.pushToStorage(wr.ctx, wr.tenantID, wr.WriteRequest)
		if err != nil {
			return fmt.Errorf("consuming record at index %d for tenant %s: %w", recordIdx, wr.tenantID, err)
		}
	}
	return nil
}

func (c pusherConsumer) pushToStorage(ctx context.Context, tenantID string, req *mimirpb.WriteRequest) error {
	spanLog, ctx := spanlogger.NewWithLogger(ctx, c.logger, "pusherConsumer.pushToStorage")
	defer spanLog.Finish()

	processingStart := time.Now()

	// Note that the implementation of the Pusher expects the tenantID to be in the context.
	ctx = user.InjectOrgID(ctx, tenantID)
	err := c.pusher.PushToStorage(ctx, req)

	// TODO dimitarvdimitrov processing time is flawed because it's only counting enqueuing time, not processing time.
	c.processingTimeSeconds.Observe(time.Since(processingStart).Seconds())
	c.totalRequests.Inc()

	isServerErr := c.handlePushErr(ctx, tenantID, err, spanLog)
	if isServerErr {
		return err
	}
	return nil
}

func (c pusherConsumer) handlePushErr(ctx context.Context, tenantID string, err error, spanLog *spanlogger.SpanLogger) bool {
	if err == nil {
		return false
	}
	// Only return non-client errors; these will stop the processing of the current Kafka fetches and retry (possibly).
	if !mimirpb.IsClientError(err) {
		c.serverErrRequests.Inc()
		_ = spanLog.Error(err)
		return true
	}

	c.clientErrRequests.Inc()

	// The error could be sampled or marked to be skipped in logs, so we check whether it should be
	// logged before doing it.
	if keep, reason := c.shouldLogClientError(ctx, err); keep {
		if reason != "" {
			err = fmt.Errorf("%w (%s)", err, reason)
		}
		// This error message is consistent with error message in Prometheus remote-write and OTLP handlers in distributors.
		level.Warn(spanLog).Log("msg", "detected a client error while ingesting write request (the request may have been partially ingested)", "user", tenantID, "insight", true, "err", err)
	}
	return false
}

// shouldLogClientError returns whether err should be logged.
func (c pusherConsumer) shouldLogClientError(ctx context.Context, err error) (bool, string) {
	var optional middleware.OptionalLogging
	if !errors.As(err, &optional) {
		// If error isn't sampled yet, we wrap it into our sampler and try again.
		err = c.fallbackClientErrSampler.WrapError(err)
		if !errors.As(err, &optional) {
			// We can get here if c.clientErrSampler is nil.
			return true, ""
		}
	}

	return optional.ShouldLog(ctx)
}

// The passed context is expected to be cancelled after all items in records were fully processed and are ready
// to be released. This so to guaranty the release of resources associated with each parsedRecord context.
func (c pusherConsumer) unmarshalRequests(ctx context.Context, records []record, recC chan<- parsedRecord) {
	defer close(recC)
	done := ctx.Done()

	for _, rec := range records {
		// rec.ctx contains the tracing baggage for this record, which we propagate down the call tree.
		// Since rec.ctx cancellation is disjointed from the context passed to unmarshalRequests(), the context.AfterFunc below,
		// fuses the two lifetimes together.
		recCtx, cancelRecCtx := context.WithCancelCause(rec.ctx)
		context.AfterFunc(ctx, func() {
			cancelRecCtx(context.Cause(ctx))
		})
		pRecord := parsedRecord{
			ctx:          recCtx,
			tenantID:     rec.tenantID,
			WriteRequest: &mimirpb.WriteRequest{},
		}
		// We don't free the WriteRequest slices because they are being freed by the Pusher.
		err := pRecord.WriteRequest.Unmarshal(rec.content)
		if err != nil {
			pRecord.err = fmt.Errorf("parsing ingest consumer write request: %w", err)
		}
		select {
		case <-done:
			return
		case recC <- pRecord:
		}
	}
}

type multiTenantPusher struct {
	pushers               map[string]*shardingPusher
	upstreamPusher        Pusher
	numShards             int
	batchSize             int
	numTimeSeriesPerFlush prometheus.Histogram
}

func (c multiTenantPusher) PushToStorage(ctx context.Context, request *mimirpb.WriteRequest) error {
	user, _ := user.ExtractOrgID(ctx)
	return c.pusher(user).PushToStorage(ctx, request)
}

// TODO dimitarvdimitrov rename because this is multi-tenant sharding pusher
func newMultiTenantPusher(numTimeSeriesPerFlush prometheus.Histogram, upstream Pusher, numShards int, batchSize int) *multiTenantPusher {
	return &multiTenantPusher{
		pushers:               make(map[string]*shardingPusher),
		upstreamPusher:        upstream,
		numShards:             numShards,
		batchSize:             batchSize,
		numTimeSeriesPerFlush: numTimeSeriesPerFlush,
	}
}

func (c multiTenantPusher) pusher(userID string) *shardingPusher {
	if p := c.pushers[userID]; p != nil {
		return p
	}
	p := newShardingPusher(c.numTimeSeriesPerFlush, c.numShards, c.batchSize, c.upstreamPusher) // TODO dimitarvdimitrov this ok or do we need to inject a factory here too?
	c.pushers[userID] = p
	return p
}

func (c multiTenantPusher) Close() []error {
	var errs multierror.MultiError
	for _, p := range c.pushers {
		errs.Add(p.close())
	}
	clear(c.pushers)
	return errs
}

type shardedPush struct {
	mimirpb.PreallocTimeseries
	context.Context
}

type shardingPusher struct {
	numShards             int
	shards                []chan shardedPush
	upstream              Pusher
	wg                    *sync.WaitGroup
	errs                  chan error
	batchSize             int
	numTimeSeriesPerFlush prometheus.Histogram
}

func newShardingPusher(numTimeSeriesPerFlush prometheus.Histogram, numShards int, batchSize int, upstream Pusher) *shardingPusher {
	pusher := &shardingPusher{
		numShards:             numShards,
		upstream:              upstream,
		numTimeSeriesPerFlush: numTimeSeriesPerFlush,
		batchSize:             batchSize,
		wg:                    &sync.WaitGroup{},
		errs:                  make(chan error, numShards),
	}
	shards := make([]chan shardedPush, numShards)
	pusher.wg.Add(numShards)
	for i := range shards {
		shards[i] = make(chan shardedPush)
		go pusher.runShard(shards[i])
	}
	go func() {
		pusher.wg.Wait()
		close(pusher.errs)
	}()

	pusher.shards = shards
	return pusher
}

// TODO dimitarvdimitrov consider having this long-lived and not closing it.
func (p *shardingPusher) PushToStorage(ctx context.Context, request *mimirpb.WriteRequest) error {
	var (
		builder         labels.ScratchBuilder
		nonCopiedLabels labels.Labels
		errs            multierror.MultiError
	)
	for _, ts := range request.Timeseries {
		mimirpb.FromLabelAdaptersOverwriteLabels(&builder, ts.Labels, &nonCopiedLabels)
		shard := nonCopiedLabels.Hash() % uint64(p.numShards)

	tryPush:
		for {
			select {
			case p.shards[shard] <- shardedPush{ts, ctx}:
				break tryPush
			case err := <-p.errs:
				errs.Add(err)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return errs.Err()
}

// TODO dimitarvdimitrov support metadata
func (p *shardingPusher) runShard(timeseries chan shardedPush) {
	defer p.wg.Done()
	timeseriesBatch := mimirpb.PreallocTimeseriesSliceFromPool()

	flush := func(ctx context.Context) {
		p.numTimeSeriesPerFlush.Observe(float64(len(timeseriesBatch)))
		err := p.upstream.PushToStorage(ctx, &mimirpb.WriteRequest{Timeseries: timeseriesBatch})
		if err != nil {
			p.errs <- err
		}
		timeseriesBatch = mimirpb.PreallocTimeseriesSliceFromPool()
	}

	var lastCtx context.Context
	for ts := range timeseries {
		timeseriesBatch = append(timeseriesBatch, ts.PreallocTimeseries)
		if len(timeseriesBatch) == p.batchSize {
			// TODO dimitarvdimitrov this breaks tracing because we might not use the spans from all of the requests
			flush(ts.Context)
		}
		lastCtx = ts.Context
	}
	if len(timeseriesBatch) > 0 {
		flush(lastCtx)
	}
}

func (p *shardingPusher) close() error {
	for _, shard := range p.shards {
		close(shard)
	}
	var errs multierror.MultiError
	for err := range p.errs {
		errs.Add(err)
	}
	return errs.Err()
}
