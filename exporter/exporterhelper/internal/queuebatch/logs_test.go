// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queuebatch // import "go.opentelemetry.io/collector/exporter/exporterhelper/internal/queuebatch"

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/testdata"
)

func TestLogsRequest(t *testing.T) {
	lr := newLogsRequest(testdata.GenerateLogs(1))

	logErr := consumererror.NewLogs(errors.New("some error"), plog.NewLogs())
	assert.Equal(
		t,
		newLogsRequest(plog.NewLogs()),
		lr.(request.ErrorHandler).OnError(logErr),
	)
}

func TestLogsRequest_Size(t *testing.T) {
	ld := testdata.GenerateLogs(1)
	req := newLogsRequest(ld).(*logsRequest)

	szt := request.SizerTypeItems
	szr := &mockLogsSizer{size: 42}

	// Initially not cached
	assert.Equal(t, 42, req.size(szt, szr))

	// Now it should be cached, change szr to return something else to verify cache is used
	szr.size = 100
	assert.Equal(t, 42, req.size(szt, szr))
}

func TestLogsRequest_SetCachedSize(t *testing.T) {
	ld := testdata.GenerateLogs(1)
	req := newLogsRequest(ld).(*logsRequest)

	szt := request.SizerTypeItems

	req.setCachedSize(szt, 99)

	// Verify it returns the cached size without calling sizer
	szr := &mockLogsSizer{size: 42}
	assert.Equal(t, 99, req.size(szt, szr))
}

type mockLogsSizer struct {
	size int
}

func (m *mockLogsSizer) LogsSize(_ plog.Logs) int {
	return m.size
}

func (m *mockLogsSizer) ResourceLogsSize(_ plog.ResourceLogs) int { return 0 }
func (m *mockLogsSizer) ScopeLogsSize(_ plog.ScopeLogs) int       { return 0 }
func (m *mockLogsSizer) LogRecordSize(_ plog.LogRecord) int       { return 0 }
func (m *mockLogsSizer) DeltaSize(newItemSize int) int            { return newItemSize }
