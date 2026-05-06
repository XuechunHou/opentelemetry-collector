// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queuebatch // import "go.opentelemetry.io/collector/exporter/exporterhelper/internal/queuebatch"

import (
	"context"
	"sync"
	"time"

	"go.uber.org/multierr"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/queue"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/sender"
)

// partitionIdleCycles*FlushTimeout is the duration after which an empty partition is removed.
// TODO make this configurable.
const partitionIdleCycles = 10

var _ Batcher[request.Request] = (*partitionBatcher)(nil)

type batch struct {
	ctx  context.Context
	req  request.Request
	done multiDone
}

// partitionBatcher continuously batch incoming requests and flushes asynchronously if minimum size limit is met or on timeout.
type partitionBatcher struct {
	cfg            BatchConfig
	wp             *workerPool
	sizers         map[request.SizerType]request.Sizer
	mergeCtx       func(context.Context, context.Context) context.Context
	consumeFunc    sender.SendFunc[request.Request]
	stopWG         sync.WaitGroup
	currentBatchMu sync.Mutex
	currentBatch   *batch
	timer          *time.Timer
	shutdownCh     chan struct{}
	logger         *zap.Logger
	onEmpty        func()    // callback triggered when partition is idle for given time period.
	lastDataTime   time.Time // tracks when data was last present
	active         bool      // indicates if partition is still active i.e timer is running and shutdown is not called yet. If Consume is called on inactive partition then data is flushed sync because timer is not running.
}

func newPartitionBatcher(
	cfg BatchConfig,
	mergeCtx func(context.Context, context.Context) context.Context,
	wp *workerPool,
	next sender.SendFunc[request.Request],
	logger *zap.Logger,
	onEmpty func(),
) *partitionBatcher {

	sizers := make(map[request.SizerType]request.Sizer)
	for szt := range cfg.Sizers {
		sizers[szt] = request.NewSizer(szt)
	}
	return &partitionBatcher{
		cfg:          cfg,
		wp:           wp,
		sizers:       sizers,
		mergeCtx:     mergeCtx,
		consumeFunc:  next,
		shutdownCh:   make(chan struct{}, 1),
		logger:       logger,
		onEmpty:      onEmpty,
		lastDataTime: time.Now(),
		active:       true,
	}
}

func (qb *partitionBatcher) shouldKeep(req request.Request) bool {
	if len(qb.cfg.Sizers) == 0 {
		return false
	}
	for szt, limit := range qb.cfg.Sizers {
		sz := qb.sizers[szt]
		if sz.Sizeof(req) >= limit.MinSize {
			return false
		}
	}
	return true
}

func (qb *partitionBatcher) splitRequest(ctx context.Context, req request.Request) ([]request.Request, error) {
	reqs := []request.Request{req}
	var firstErr error
	for szt, limit := range qb.cfg.Sizers {
		if limit.MaxSize <= 0 {
			continue
		}
		var newReqs []request.Request
		for _, r := range reqs {
			chunks, err := r.MergeSplit(ctx, int(limit.MaxSize), szt, nil)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
			}
			newReqs = append(newReqs, chunks...)
		}
		reqs = newReqs
	}
	return reqs, firstErr
}

func (qb *partitionBatcher) mergeAndSplit(ctx context.Context, req1, req2 request.Request) ([]request.Request, error) {
	var firstSzt request.SizerType
	var firstLimit SizerLimit
	found := false
	for szt, limit := range qb.cfg.Sizers {
		if limit.MaxSize > 0 {
			firstSzt = szt
			firstLimit = limit
			found = true
			break
		}
	}

	var reqs []request.Request
	var firstErr error
	if found {
		reqs, firstErr = req1.MergeSplit(ctx, int(firstLimit.MaxSize), firstSzt, req2)
	} else {
		szt := request.SizerTypeItems
		for k := range qb.cfg.Sizers {
			szt = k
			break
		}
		reqs, firstErr = req1.MergeSplit(ctx, 0, szt, req2)
	}

	for szt, limit := range qb.cfg.Sizers {
		if found && szt == firstSzt {
			continue
		}
		if limit.MaxSize <= 0 {
			continue
		}
		var newReqs []request.Request
		for _, r := range reqs {
			chunks, err := r.MergeSplit(ctx, int(limit.MaxSize), szt, nil)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
			}
			newReqs = append(newReqs, chunks...)
		}
		reqs = newReqs
	}

	return reqs, firstErr
}

func (qb *partitionBatcher) resetTimer() {
	if qb.cfg.FlushTimeout > 0 {
		qb.timer.Reset(qb.cfg.FlushTimeout)
	}
}

func (qb *partitionBatcher) consumeInternal(ctx context.Context, req request.Request, done queue.Done) bool {
	qb.currentBatchMu.Lock()
	isActive := qb.active
	qb.lastDataTime = time.Now()
	if qb.currentBatch == nil {
		reqList, mergeSplitErr := qb.splitRequest(ctx, req)
		if mergeSplitErr != nil {
			// Do not return in case of error if there are data, try to export as much as possible.
			qb.logger.Warn("Failed to split request.", zap.Error(mergeSplitErr))
		}

		if len(reqList) == 0 {
			done.OnDone(mergeSplitErr)
			qb.currentBatchMu.Unlock()
			return isActive
		}

		// If more than one flush is required for this request, call done only when all flushes are done.
		numRefs := len(reqList)
		// Need to also inform about the mergeSplitErr, consider the errored data as 1 batch.
		if mergeSplitErr != nil {
			numRefs++
		}
		if numRefs > 1 {
			done = newRefCountDone(done, int64(numRefs))
			if mergeSplitErr != nil {
				done.OnDone(mergeSplitErr)
			}
		}

		// We have at least one result in the reqList. Last in the list may not have enough data to be flushed.
		// Find if it has at least MinSize, and if it does then move that as the current batch.
		lastReq := reqList[len(reqList)-1]
		if qb.shouldKeep(lastReq) {
			// Do not flush the last item and add it to the current batch.
			reqList = reqList[:len(reqList)-1]
			qb.currentBatch = &batch{
				ctx:  ctx,
				req:  lastReq,
				done: multiDone{done},
			}
			qb.resetTimer()
		}

		qb.currentBatchMu.Unlock()
		for i := 0; i < len(reqList); i++ {
			qb.flush(ctx, reqList[i], done)
		}

		return isActive
	}

	reqList, mergeSplitErr := qb.mergeAndSplit(ctx, qb.currentBatch.req, req)
	// If failed to merge signal all Done callbacks from the current batch as well as the current request and reset the current batch.
	if mergeSplitErr != nil {
		// Do not return in case of error if there are data, try to export as much as possible.
		qb.logger.Warn("Failed to split request.", zap.Error(mergeSplitErr))
	}

	if len(reqList) == 0 {
		done.OnDone(mergeSplitErr)
		qb.currentBatchMu.Unlock()
		return isActive
	}

	// If more than one flush is required for this request, call done only when all flushes are done.
	numRefs := len(reqList)
	// Need to also inform about the mergeSplitErr, consider the errored data as 1 batch.
	if mergeSplitErr != nil {
		numRefs++
	}
	if numRefs > 1 {
		done = newRefCountDone(done, int64(numRefs))
		if mergeSplitErr != nil {
			done.OnDone(mergeSplitErr)
		}
	}

	// We have at least one result in the reqList, if more results here is what that means:
	// - First result will contain items from the current batch + some results from the current request.
	// - All other results except first will contain items only from the current request.
	// - Last result may not have enough data to be flushed.

	// Logic on how to deal with the current batch:
	qb.currentBatch.req = reqList[0]
	qb.currentBatch.done = append(qb.currentBatch.done, done)

	mergedCtx := context.Background() //nolint:contextcheck
	if qb.mergeCtx != nil {
		mergedCtx = qb.mergeCtx(qb.currentBatch.ctx, ctx)
	}
	qb.currentBatch.ctx = contextWithMergedLinks(mergedCtx, qb.currentBatch.ctx, ctx)

	// Save the "currentBatch" if we need to flush it, because we want to execute flush without holding the lock, and
	// cannot unlock and re-lock because we are not done processing all the responses.
	var firstBatch *batch
	// Need to check the currentBatch if more than 1 result returned or if 1 result return but larger than MinSize.
	if len(reqList) > 1 || !qb.shouldKeep(qb.currentBatch.req) {
		firstBatch = qb.currentBatch
		qb.currentBatch = nil
	}
	// At this moment we dealt with the first result which is iter in the currentBatch or in the `firstBatch` we will flush.
	reqList = reqList[1:]

	// If we still have results to process, then we need to check if the last result has enough data to flush, or we add it to the currentBatch.
	if len(reqList) > 0 {
		lastReq := reqList[len(reqList)-1]
		if qb.shouldKeep(lastReq) {
			// Do not flush the last item and add it to the current batch.
			reqList = reqList[:len(reqList)-1]
			qb.currentBatch = &batch{
				ctx:  ctx,
				req:  lastReq,
				done: multiDone{done},
			}
			qb.resetTimer()
		}
	}

	qb.currentBatchMu.Unlock()
	if firstBatch != nil {
		qb.flush(firstBatch.ctx, firstBatch.req, firstBatch.done)
	}
	for i := 0; i < len(reqList); i++ {
		qb.flush(ctx, reqList[i], done)
	}
	return isActive
}

func (qb *partitionBatcher) Consume(ctx context.Context, req request.Request, done queue.Done) {
	if !qb.consumeInternal(ctx, req, done) {
		// Not active partition then flush else let the timer/Shutdown do it's work.
		qb.flushCurrentBatchOrRemovePartition()
	}
}

// Start starts the goroutine that reads from the queue and flushes asynchronously.
func (qb *partitionBatcher) Start(context.Context, component.Host) error {
	if qb.cfg.FlushTimeout <= 0 {
		return nil
	}
	qb.timer = time.NewTimer(qb.cfg.FlushTimeout)
	qb.stopWG.Go(func() {
		for {
			select {
			case <-qb.shutdownCh:
				return
			case <-qb.timer.C:
				qb.flushCurrentBatchOrRemovePartition()
			}
		}
	})
	return nil
}

// shutdownInternal ensures that queue and all Batcher are stopped.
func (qb *partitionBatcher) shutdownInternal() {
	qb.currentBatchMu.Lock()
	if !qb.active {
		qb.currentBatchMu.Unlock()
		return
	}
	qb.active = false
	// don't need to trigger onEmpty during shutdown as partitionBatcher will be purged anyway.
	qb.onEmpty = nil
	qb.currentBatchMu.Unlock()
	close(qb.shutdownCh)
	// Make sure execute one last flush if necessary.
	qb.flushCurrentBatchOrRemovePartition()
	qb.stopWG.Wait()
}

// Shutdown ensures that queue and all Batcher are stopped.
func (qb *partitionBatcher) Shutdown(context.Context) error {
	qb.shutdownInternal()
	return nil
}

// flushCurrentBatchOrRemovePartition flushes the current batch if not empty,
// or removes the partition from the parent if it's been idle for too long.
func (qb *partitionBatcher) flushCurrentBatchOrRemovePartition() {
	qb.currentBatchMu.Lock()
	if qb.currentBatch == nil {
		// No data to flush - check if idle for too long AND no one holding a reference
		idleDuration := time.Since(qb.lastDataTime)

		if idleDuration >= (partitionIdleCycles*qb.cfg.FlushTimeout) && qb.onEmpty != nil {
			qb.currentBatchMu.Unlock()
			qb.onEmpty()
			return
		}
		if qb.timer != nil {
			qb.resetTimer()
		}
		qb.currentBatchMu.Unlock()
		return
	}

	// Has data to flush - update lastDataTime
	qb.lastDataTime = time.Now()
	batchToFlush := qb.currentBatch
	qb.currentBatch = nil
	// Reset timer while holding the lock to prevent data race with Consume() which
	// also calls resetTimer() under the same lock.
	qb.resetTimer()
	qb.currentBatchMu.Unlock()
	// flush() blocks until successfully started a goroutine for flushing.
	qb.flush(batchToFlush.ctx, batchToFlush.req, batchToFlush.done)
}

// flush starts a goroutine that calls consumeFunc. It blocks until a worker is available if necessary.
func (qb *partitionBatcher) flush(ctx context.Context, req request.Request, done queue.Done) {
	qb.stopWG.Add(1)
	qb.wp.execute(func() {
		defer qb.stopWG.Done()
		done.OnDone(qb.consumeFunc(ctx, req))
	})
}

type workerPool struct {
	workers chan struct{}
}

func newWorkerPool(maxWorkers int) *workerPool {
	workers := make(chan struct{}, maxWorkers)
	for range maxWorkers {
		workers <- struct{}{}
	}
	return &workerPool{workers: workers}
}

func (wp *workerPool) execute(f func()) {
	<-wp.workers
	go func() {
		defer func() {
			wp.workers <- struct{}{}
		}()
		f()
	}()
}

type multiDone []queue.Done

func (mdc multiDone) OnDone(err error) {
	for _, d := range mdc {
		d.OnDone(err)
	}
}

type refCountDone struct {
	done     queue.Done
	mu       sync.Mutex
	refCount int64
	err      error
}

func newRefCountDone(done queue.Done, refCount int64) queue.Done {
	return &refCountDone{
		done:     done,
		refCount: refCount,
	}
}

func (rcd *refCountDone) OnDone(err error) {
	rcd.mu.Lock()
	defer rcd.mu.Unlock()
	rcd.err = multierr.Append(rcd.err, err)
	rcd.refCount--
	if rcd.refCount == 0 {
		// No more references, call done.
		rcd.done.OnDone(rcd.err)
	}
}
