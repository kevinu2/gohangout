package filter

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/kevinu2/gohangout/topology"
)

func createLinkStatsMetricEvents(now int64) []map[string]interface{} {
	return createLinkMetricEvents(now)
}

func TestLinkStatsMetricFilter(t *testing.T) {
	var (
		config            map[interface{}]interface{}
		f                 *LinkStatsMetricFilter
		ok                bool
		batchWindow       = 5
		reserveWindow     = 20
		windowOffset      = 0
		ts                int64
		dropOriginalEvent = true
	)

	config = make(map[interface{}]interface{})
	config["fieldsLink"] = "host->request_statusCode->responseTime"
	config["reserveWindow"] = reserveWindow
	config["batchWindow"] = batchWindow
	config["windowOffset"] = windowOffset
	config["drop_original_event"] = dropOriginalEvent

	f = (BuildFilter("LinkStatsMetric", config)).(*LinkStatsMetricFilter)
	f.SetBelongTo(topology.NewFilterBox(config))

	now := time.Now().Unix()
	for _, event := range createLinkStatsMetricEvents(now) {
		f.Filter(event)
	}

	t.Logf("metric: %v", f.metric)
	b, _ := json.Marshal(f.metric)
	t.Logf("metric: %s", b)

	if ok == true {
		t.Error("LinkStatsMetric filter process should return false")
	}

	if len(f.metric) != 4 {
		t.Errorf("%v", f.metricToEmit[ts])
	}

	f.swapMetricMetricateMit()
	t.Logf("metricToEmit %v", f.metricToEmit)
	if len(f.metricToEmit) != 4 {
		t.Error(f.metricToEmit)
	}

	_15 := now - 15
	ts = _15 - _15%(int64)(batchWindow)
	if f.metricToEmit[ts] == nil {
		t.Errorf("_15 should be in metricToEmit")
	}
	_5 := now - 5
	ts = _5 - _5%(int64)(batchWindow)
	if f.metricToEmit[ts] == nil {
		t.Errorf("_5 should be in metricToEmit")
	}
	_0 := now
	ts = _0 - _0%(int64)(batchWindow)
	if f.metricToEmit[ts] == nil {
		t.Errorf("_0 should be in metricToEmit")
	}

	_10 := now - 10
	ts = _10 - _10%(int64)(batchWindow)
	metric := f.metricToEmit[ts].(map[interface{}]interface{})
	if len(metric) != 2 {
		t.Errorf("_10 metric length should be 2")
	}
	if metric["localhost"] == nil {
		t.Errorf("localhost should be in _10 metric")
	}
	if metric["remote"] == nil {
		t.Errorf("remote should be in _10 metric")
	}

	localhostMetric := f.metricToEmit[ts].(map[interface{}]interface{})["localhost"].(map[interface{}]interface{})
	if len(localhostMetric) != 2 {
		t.Errorf("localhost metric length should be 2")
	}
	if localhostMetric["200"] == nil {
		t.Errorf("200 should be in localhost_metric")
	}
	if localhostMetric["301"] == nil {
		t.Errorf("301 should be in localhost_metric")
	}

	if len(localhostMetric["200"].(map[interface{}]interface{})) != 1 {
		t.Errorf("%v", f.metricToEmit[ts])
	}
	if localhostMetric["200"].(map[interface{}]interface{})["responseTime"].(*stats).count != 2 {
		t.Errorf("_10->localhost->200->responseTime-count should be 2")
	}
	if localhostMetric["200"].(map[interface{}]interface{})["responseTime"].(*stats).sum != 20.4 {
		t.Errorf("_10->localhost->200->responseTime-sum should be 20.4")
	}
}

func TestLinkStatsMetricFilterWindowOffset(t *testing.T) {
	var (
		config            map[interface{}]interface{}
		f                 *LinkStatsMetricFilter
		ok                bool
		batchWindow       = 5
		reserveWindow     = 20
		windowOffset      = 2
		ts                int64
		dropOriginalEvent = true
	)

	config = make(map[interface{}]interface{})
	config["fieldsLink"] = "host->request_statusCode->responseTime"
	config["reserveWindow"] = reserveWindow
	config["batchWindow"] = batchWindow
	config["windowOffset"] = windowOffset
	config["drop_original_event"] = dropOriginalEvent

	f = (BuildFilter("LinkStatsMetric", config)).(*LinkStatsMetricFilter)
	f.SetBelongTo(topology.NewFilterBox(config))

	now := time.Now().Unix()
	for _, event := range createLinkStatsMetricEvents(now) {
		f.Filter(event)
	}

	t.Logf("metric: %v", f.metric)
	b, _ := json.Marshal(f.metric)
	t.Logf("metric: %s", b)

	if ok == true {
		t.Error("LinkStatsMetric filter process should return false")
	}

	if len(f.metric) != 4 {
		t.Errorf("%v", f.metricToEmit[ts])
	}

	f.swapMetricMetricateMit()
	t.Logf("metricToEmit %v", f.metricToEmit)
	if len(f.metricToEmit) != 2 {
		t.Error(f.metricToEmit)
	}

	_15 := now - 15
	ts = _15 - _15%(int64)(batchWindow)
	if f.metricToEmit[ts] == nil {
		t.Errorf("_15 should be in metricToEmit")
	}
	_5 := now - 5
	ts = _5 - _5%(int64)(batchWindow)
	if f.metricToEmit[ts] != nil {
		t.Errorf("_5 should not be in metricToEmit")
	}
	_0 := now
	ts = _0 - _0%(int64)(batchWindow)
	if f.metricToEmit[ts] != nil {
		t.Errorf("_0 should not be in metricToEmit")
	}

	_10 := now - 10
	ts = _10 - _10%(int64)(batchWindow)
	metric := f.metricToEmit[ts].(map[interface{}]interface{})
	if len(metric) != 2 {
		t.Errorf("_10 metric length should be 2")
	}
	if metric["localhost"] == nil {
		t.Errorf("localhost should be in _10 metric")
	}
	if metric["remote"] == nil {
		t.Errorf("remote should be in _10 metric")
	}

	localhostMetric := f.metricToEmit[ts].(map[interface{}]interface{})["localhost"].(map[interface{}]interface{})
	if len(localhostMetric) != 2 {
		t.Errorf("localhost metric length should be 2")
	}
	if localhostMetric["200"] == nil {
		t.Errorf("200 should be in localhost_metric")
	}
	if localhostMetric["301"] == nil {
		t.Errorf("301 should be in localhost_metric")
	}

	if len(localhostMetric["200"].(map[interface{}]interface{})) != 1 {
		t.Errorf("%v", f.metricToEmit[ts])
	}
	if localhostMetric["200"].(map[interface{}]interface{})["responseTime"].(*stats).count != 2 {
		t.Errorf("_10->localhost->200->responseTime-count should be 2")
	}
	if localhostMetric["200"].(map[interface{}]interface{})["responseTime"].(*stats).sum != 20.4 {
		t.Errorf("_10->localhost->200->responseTime-sum should be 20.4")
	}
}

func _testLinkStatsMetricFilterAccumulateMode(t *testing.T, f *LinkStatsMetricFilter, now int64, count int, sum float64) {
	if len(f.metricToEmit) != 4 {
		t.Error(f.metricToEmit)
	}

	var ts int64
	_15 := now - 15
	ts = _15 - _15%f.batchWindow
	if f.metricToEmit[ts] == nil {
		t.Errorf("_15 should be in metricToEmit")
	}
	_5 := now - 5
	ts = _5 - _5%f.batchWindow
	if f.metricToEmit[ts] == nil {
		t.Errorf("_5 should be in metricToEmit")
	}
	_0 := now
	ts = _0 - _0%f.batchWindow
	if f.metricToEmit[ts] == nil {
		t.Errorf("_0 should be in metricToEmit")
	}

	_10 := now - 10
	ts = _10 - _10%f.batchWindow
	metric := f.metricToEmit[ts].(map[interface{}]interface{})
	if len(metric) != 2 {
		t.Errorf("_10 metric length should be 2")
	}
	if metric["localhost"] == nil {
		t.Errorf("localhost should be in _10 metric")
	}
	if metric["remote"] == nil {
		t.Errorf("remote should be in _10 metric")
	}

	localhostMetric := f.metricToEmit[ts].(map[interface{}]interface{})["localhost"].(map[interface{}]interface{})
	if len(localhostMetric) != 2 {
		t.Errorf("localhost metric length should be 2")
	}
	if localhostMetric["200"] == nil {
		t.Errorf("200 should be in localhost_metric")
	}
	if localhostMetric["301"] == nil {
		t.Errorf("301 should be in localhost_metric")
	}

	if len(localhostMetric["200"].(map[interface{}]interface{})) != 1 {
		t.Errorf("%v", f.metricToEmit[ts])
	}
	if localhostMetric["200"].(map[interface{}]interface{})["responseTime"].(*stats).count != count {
		t.Errorf("_10->localhost->200->responseTime-count should be %d", count)
	}
	if localhostMetric["200"].(map[interface{}]interface{})["responseTime"].(*stats).sum != sum {
		t.Errorf("_10->localhost->200->responseTime-sum should be %f", sum)
	}
}

func TestLinkStatsMetricFilterCumulativeMode(t *testing.T) {
	var (
		config            map[interface{}]interface{}
		f                 *LinkStatsMetricFilter
		batchWindow                   = 5
		reserveWindow                 = 20
		windowOffset                  = 0
		accumulateMode    interface{} = "cumulative"
		dropOriginalEvent             = true
	)

	config = make(map[interface{}]interface{})
	config["fieldsLink"] = "host->request_statusCode->responseTime"
	config["reserveWindow"] = reserveWindow
	config["batchWindow"] = batchWindow
	config["windowOffset"] = windowOffset
	config["accumulateMode"] = accumulateMode
	config["drop_original_event"] = dropOriginalEvent

	f = (BuildFilter("LinkStatsMetric", config)).(*LinkStatsMetricFilter)
	f.SetBelongTo(topology.NewFilterBox(config))

	now := time.Now().Unix()
	for _, event := range createLinkStatsMetricEvents(now) {
		f.Filter(event)
	}
	f.swapMetricMetricateMit()
	for _, event := range createLinkStatsMetricEvents(now) {
		f.Filter(event)
	}

	f.swapMetricMetricateMit()
	t.Logf("metricToEmit: %v", f.metricToEmit)

	_testLinkStatsMetricFilterAccumulateMode(t, f, now, 4, 40.8)
}

func TestLinkStatsMetricFilterSeparateMode(t *testing.T) {
	var (
		config            map[interface{}]interface{}
		f                 *LinkStatsMetricFilter
		batchWindow                   = 5
		reserveWindow                 = 20
		windowOffset                  = 0
		accumulateMode    interface{} = "separate"
		dropOriginalEvent             = true
	)

	config = make(map[interface{}]interface{})
	config["fieldsLink"] = "host->request_statusCode->responseTime"
	config["reserveWindow"] = reserveWindow
	config["batchWindow"] = batchWindow
	config["windowOffset"] = windowOffset
	config["accumulateMode"] = accumulateMode
	config["drop_original_event"] = dropOriginalEvent

	f = (BuildFilter("LinkStatsMetric", config)).(*LinkStatsMetricFilter)
	f.SetBelongTo(topology.NewFilterBox(config))

	now := time.Now().Unix()
	for _, event := range createLinkStatsMetricEvents(now) {
		f.Filter(event)
	}
	f.swapMetricMetricateMit()
	for _, event := range createLinkStatsMetricEvents(now) {
		f.Filter(event)
	}

	f.swapMetricMetricateMit()
	t.Logf("metricToEmit: %v", f.metricToEmit)
	if len(f.metricToEmit) != 4 {
		t.Error(f.metricToEmit)
	}

	_testLinkStatsMetricFilterAccumulateMode(t, f, now, 2, 20.4)
}
