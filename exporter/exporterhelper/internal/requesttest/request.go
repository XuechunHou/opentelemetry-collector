// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package requesttest // import "go.opentelemetry.io/collector/exporter/exporterhelper/internal/requesttest"

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type errorPartial struct {
	fr *FakeRequest
}

func (e errorPartial) Error() string {
	return fmt.Sprintf("items: %d", e.fr.Items)
}

type FakeRequest struct {
	Items          int
	Bytes          int
	Partial        int
	MergeErr       error
	MergeErrResult []request.Request
	Delay          time.Duration
}

func (r *FakeRequest) OnError(err error) request.Request {
	var pErr errorPartial
	if errors.As(err, &pErr) {
		return pErr.fr
	}
	return r
}

func (r *FakeRequest) ItemsCount() int {
	return r.Items
}

func (r *FakeRequest) BytesSize() int {
	return r.Bytes
}

func (r *FakeRequest) MergeSplit(_ context.Context, maxSizePerSizer map[request.SizerType]int64, r2 request.Request) ([]request.Request, error) {
	if r.MergeErr != nil {
		return r.MergeErrResult, r.MergeErr
	}

	if r2 != nil {
		fr2 := r2.(*FakeRequest)
		if fr2.MergeErr != nil {
			return fr2.MergeErrResult, fr2.MergeErr
		}
		fr2.mergeTo(r)
	}

	if len(maxSizePerSizer) == 0 {
		return []request.Request{r}, nil
	}

	var res []request.Request
	for r.Items > 0 || r.Bytes > 0 {
		chunkItems := r.Items
		chunkBytes := r.Bytes

		if limit, ok := maxSizePerSizer[request.SizerTypeItems]; ok && limit > 0 {
			if int64(chunkItems) > limit {
				chunkItems = int(limit)
				if r.Items > 0 {
					chunkBytes = r.Bytes * chunkItems / r.Items
				}
			}
		}

		if limit, ok := maxSizePerSizer[request.SizerTypeBytes]; ok && limit > 0 {
			if int64(r.Bytes) > limit {
				allowedItems := 0
				if r.Bytes > 0 {
					allowedItems = r.Items * int(limit) / r.Bytes
				}
				if allowedItems < chunkItems {
					chunkItems = allowedItems
					chunkBytes = int(limit)
				}
			}
		}

		if chunkItems == 0 && chunkBytes == 0 {
			return res, fmt.Errorf("limits too small to extract anything")
		}

		if chunkItems >= r.Items && chunkBytes >= r.Bytes {
			res = append(res, r)
			break
		}

		res = append(res, &FakeRequest{Items: chunkItems, Bytes: chunkBytes, Delay: r.Delay})
		r.Items -= chunkItems
		r.Bytes -= chunkBytes
	}

	return res, nil
}

func (r *FakeRequest) mergeTo(dst *FakeRequest) {
	dst.Items += r.Items
	dst.Bytes += r.Bytes
	dst.Delay += r.Delay
}

func RequestFromMetricsFunc(err error) func(context.Context, pmetric.Metrics) (request.Request, error) {
	return func(_ context.Context, md pmetric.Metrics) (request.Request, error) {
		return &FakeRequest{Items: md.DataPointCount()}, err
	}
}

func RequestFromTracesFunc(err error) func(context.Context, ptrace.Traces) (request.Request, error) {
	return func(_ context.Context, td ptrace.Traces) (request.Request, error) {
		return &FakeRequest{Items: td.SpanCount()}, err
	}
}

func RequestFromLogsFunc(err error) func(context.Context, plog.Logs) (request.Request, error) {
	return func(_ context.Context, ld plog.Logs) (request.Request, error) {
		return &FakeRequest{Items: ld.LogRecordCount()}, err
	}
}
