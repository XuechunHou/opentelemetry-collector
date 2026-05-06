// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queuebatch // import "go.opentelemetry.io/collector/exporter/exporterhelper/internal/queuebatch"

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/queue"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/sender"
)

// Batcher is in charge of reading items from the queue and send them out asynchronously.
type Batcher[T any] interface {
	component.Component
	Consume(context.Context, T, queue.Done)
}

type batcherSettings[T any] struct {
	partitioner Partitioner[T]
	mergeCtx    func(context.Context, context.Context) context.Context
	next        sender.SendFunc[T]
	maxWorkers  int
	logger      *zap.Logger
	QueueSizer  request.SizerType
}

func NewBatcher(cfg configoptional.Optional[BatchConfig], set batcherSettings[request.Request]) (Batcher[request.Request], error) {
	if !cfg.HasValue() {
		return newDisabledBatcher(set.next), nil
	}
	bCfg := cfg.Get()

	if len(bCfg.Sizers) > 0 {
		// Normalize the first sizer to legacy fields (should only be one because of Validate)
		for szt, limit := range bCfg.Sizers {
			bCfg.Sizer = szt
			bCfg.MinSize = limit.MinSize
			bCfg.MaxSize = limit.MaxSize
			break
		}
	} else {
		// Fallback to queue sizer
		sizerType := set.QueueSizer
		if sizerType.String() == "" {
			sizerType = request.SizerTypeItems
		}
		bCfg.Sizer = sizerType
		// MinSize and MaxSize remain 0
	}

	sizer := request.NewSizer(bCfg.Sizer)
	if sizer == nil {
		return nil, fmt.Errorf("queue_batch: unsupported sizer %q", bCfg.Sizer)
	}

	if set.partitioner == nil {
		return newPartitionBatcher(*bCfg, sizer, set.mergeCtx, newWorkerPool(set.maxWorkers), set.next, set.logger, nil), nil
	}

	mb, err := newMultiBatcher(*bCfg, sizer, newWorkerPool(set.maxWorkers), set.partitioner, set.mergeCtx, set.next, set.logger)
	if err != nil {
		return nil, fmt.Errorf("error during creating multi batcher: %w", err)
	}
	return mb, nil
}
