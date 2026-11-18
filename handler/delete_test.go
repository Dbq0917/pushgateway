// Copyright 2026 The Prometheus Authors
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

package handler

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/promslog"

	"github.com/prometheus/pushgateway/storage"
)

// scrapeMetrics renders what a Prometheus scrape of the Pushgateway would
// return for the pushed metrics, in the Prometheus text exposition format.
func scrapeMetrics(t *testing.T, ms storage.MetricStore) string {
	t.Helper()
	g := prometheus.Gatherers{
		prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
			return ms.GetMetricFamilies(), nil
		}),
	}
	mfs, err := g.Gather()
	if err != nil {
		t.Fatalf("unexpected error gathering metrics: %v", err)
	}
	buf := &bytes.Buffer{}
	enc := expfmt.NewEncoder(buf, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, mf := range mfs {
		if err := enc.Encode(mf); err != nil {
			t.Fatalf("unexpected error encoding metric family: %v", err)
		}
	}
	return buf.String()
}

func pushMetrics(t *testing.T, handler func(http.ResponseWriter, *http.Request), body string) {
	t.Helper()
	req, err := http.NewRequest("POST", "http://example.org/", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	params := map[string]string{
		"job":    "testjob",
		"labels": "/instance/testinstance",
	}
	w := httptest.NewRecorder()
	handler(w, req.WithContext(ctxWithParams(params, req)))
	if expected, got := http.StatusOK, w.Code; expected != got {
		t.Fatalf("push: wanted status code %v, got %v", expected, got)
	}
}

func deleteMetrics(t *testing.T, handler func(http.ResponseWriter, *http.Request), remoteAddr string) {
	t.Helper()
	req, err := http.NewRequest("DELETE", "http://example.org/", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.RemoteAddr = remoteAddr
	params := map[string]string{
		"job":    "testjob",
		"labels": "/instance/testinstance",
	}
	w := httptest.NewRecorder()
	handler(w, req.WithContext(ctxWithParams(params, req)))
	if expected, got := http.StatusAccepted, w.Code; expected != got {
		t.Fatalf("delete: wanted status code %v, got %v", expected, got)
	}
}

type auditRecord struct {
	Msg        string `json:"msg"`
	Job        string `json:"job"`
	Labels     string `json:"labels"`
	Existed    bool   `json:"existed"`
	RemoteAddr string `json:"remote_addr"`
}

func auditLogRecords(t *testing.T, logBuf *bytes.Buffer) []auditRecord {
	t.Helper()
	records := []auditRecord{}
	for _, line := range strings.Split(strings.TrimSpace(logBuf.String()), "\n") {
		if line == "" {
			continue
		}
		r := auditRecord{}
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("audit log line is not valid JSON: %q: %v", line, err)
		}
		records = append(records, r)
	}
	return records
}

// TestDeleteEffectiveImmediately verifies the whole delete lifecycle against
// a real MetricStore: after pushing a group and deleting it, the very next
// scrape must no longer expose the group, and a subsequent push must only
// expose the newly pushed metrics. It also verifies that every delete is
// recorded in an audit log with who (source) and what (job, labels, whether
// the group existed) was deleted.
func TestDeleteEffectiveImmediately(t *testing.T) {
	ms := storage.NewDiskMetricStore("", time.Minute, nil, promslog.NewNopLogger())
	defer ms.Shutdown()

	logBuf := &bytes.Buffer{}
	auditLogger := slog.New(slog.NewJSONHandler(logBuf, nil))

	pushHandler := Push(ms, false, true, false, promslog.NewNopLogger())
	deleteHandler := Delete(ms, false, auditLogger)

	// Push a first metric. The scrape must show it (state before delete).
	pushMetrics(t, pushHandler, "metric_a 1\n")
	before := scrapeMetrics(t, ms)
	if !strings.Contains(before, `metric_a{instance="testinstance",job="testjob"} 1`) {
		t.Errorf("scrape before delete does not contain metric_a, got:\n%s", before)
	}

	// Delete the group. The very next scrape must not show it anymore.
	deleteMetrics(t, deleteHandler, "192.0.2.1:12345")
	after := scrapeMetrics(t, ms)
	if strings.Contains(after, "metric_a") {
		t.Errorf("scrape after delete still contains metric_a, got:\n%s", after)
	}
	if strings.Contains(after, `job="testjob"`) {
		t.Errorf("scrape after delete still contains the deleted group, got:\n%s", after)
	}

	// Push a new metric into the same group. The scrape must show only the
	// new metric, nothing from before the delete.
	pushMetrics(t, pushHandler, "metric_b 2\n")
	repushed := scrapeMetrics(t, ms)
	if !strings.Contains(repushed, `metric_b{instance="testinstance",job="testjob"} 2`) {
		t.Errorf("scrape after re-push does not contain metric_b, got:\n%s", repushed)
	}
	if strings.Contains(repushed, "metric_a") {
		t.Errorf("scrape after re-push unexpectedly contains metric_a, got:\n%s", repushed)
	}

	// Delete the re-pushed group, then delete it once more. Deleting a
	// non-existing group is still accepted, but the audit log records
	// that the group did not exist.
	deleteMetrics(t, deleteHandler, "192.0.2.1:12345")
	deleteMetrics(t, deleteHandler, "192.0.2.1:12345")

	records := auditLogRecords(t, logBuf)
	if expected, got := 3, len(records); expected != got {
		t.Fatalf("wanted %d audit log records, got %d: %v", expected, got, records)
	}
	for i, wantExisted := range []bool{true, true, false} {
		r := records[i]
		if r.Msg != "metric group deleted" {
			t.Errorf("record %d: unexpected msg %q", i, r.Msg)
		}
		if r.Job != "testjob" {
			t.Errorf("record %d: unexpected job %q", i, r.Job)
		}
		if r.Labels != "/instance/testinstance" {
			t.Errorf("record %d: unexpected labels %q", i, r.Labels)
		}
		if r.RemoteAddr != "192.0.2.1:12345" {
			t.Errorf("record %d: unexpected remote_addr %q", i, r.RemoteAddr)
		}
		if r.Existed != wantExisted {
			t.Errorf("record %d: wanted existed=%v, got %v", i, wantExisted, r.Existed)
		}
	}
}
