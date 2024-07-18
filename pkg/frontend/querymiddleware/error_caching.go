package querymiddleware

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"time"

	apierror "github.com/grafana/mimir/pkg/api/error"
)

const (
	reasonNotError            = "not-error"
	reasonNotApiError         = "not-api-error"
	reasonNotCacheableError   = "not-cacheable-api-error"
	reasonNotCacheableRequest = "not-cacheable-request" // TODO: ??? how do we match this up with the request reasons? include them?
)

func newErrorCachingMiddleware(cache cache.Cache, limits Limits, ttl time.Duration, logger log.Logger, reg prometheus.Registerer) MetricsQueryMiddleware {
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &errorCachingHandler{
			next:   next,
			cache:  cache,
			limits: limits,
			ttl:    ttl,
			logger: logger,
		}
	})
}

type errorCachingHandler struct {
	next   MetricsQueryHandler
	cache  cache.Cache
	limits Limits
	ttl    time.Duration
	logger log.Logger

	requestsTotal     prometheus.Counter
	requestsNotCached prometheus.CounterVec
}

func (e *errorCachingHandler) Do(ctx context.Context, request MetricsQueryRequest) (Response, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return e.next.Do(ctx, request)
	}

	key := e.cacheKey(tenant.JoinTenantIDs(tenantIDs), request)
	hashedKey := cacheHashKey(key)

	cachedErr := e.loadErrorFromCache(ctx, key, hashedKey)
	if cachedErr != nil {
		level.Info(e.logger).Log("msg", "!!! returned cached API error", "api_err", cachedErr, "key", key, "hashed_key", hashedKey)
		return nil, cachedErr
	}

	res, err := e.next.Do(ctx, request)
	if err != nil {
		var apiErr *apierror.APIError
		if !errors.As(err, &apiErr) {
			level.Info(e.logger).Log("msg", "!!! result of request is non-API error", "err", err)
			return res, err
		}

		maxCacheFreshness := validation.MaxDurationPerTenant(tenantIDs, e.limits.MaxCacheFreshness)
		maxCacheTime := int64(model.Now().Add(-maxCacheFreshness))
		cacheUnalignedRequests := validation.AllTrueBooleansPerTenant(tenantIDs, e.limits.ResultsCacheForUnalignedQueryEnabled)

		cacheableErr := e.isErrorCacheable(apiErr)
		cacheableReq, reason := isRequestCachable(request, maxCacheTime, cacheUnalignedRequests, e.logger)

		if cacheableErr && cacheableReq {
			level.Info(e.logger).Log("msg", "!!! result of request is an error that can be cached", "api_err", apiErr)
			e.storeErrorToCache(key, hashedKey, apiErr)
		} else {
			level.Info(e.logger).Log(
				"msg", "!!! result of request is a non-cacheable API error",
				"api_err", apiErr,
				"cacheable_err", cacheableErr,
				"cacheable_req", cacheableReq,
				"reason", reason,
			)
		}
	} else {
		level.Info(e.logger).Log("msg", "!!! success response, not caching here")
	}

	return res, err
}

func (e *errorCachingHandler) loadErrorFromCache(ctx context.Context, key, hashedKey string) *apierror.APIError {
	return nil // no-op
}

func (e *errorCachingHandler) storeErrorToCache(key, hashedKey string, err *apierror.APIError) {
	// no-op
}

func (e *errorCachingHandler) cacheKey(tenantID string, r MetricsQueryRequest) string {
	return fmt.Sprintf("EC:%s:%s:%d:%d:%d", tenantID, r.GetQuery(), r.GetStart(), r.GetEnd(), r.GetStep())
}

func (e *errorCachingHandler) isErrorCacheable(apiErr *apierror.APIError) bool {
	return apiErr.Type == apierror.TypeBadData || apiErr.Type == apierror.TypeExec
}
