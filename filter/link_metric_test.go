package filter

import (
	"testing"
	"time"

	"github.com/kevinu2/gohangout/topology"
)

func createLinkMetricEvents(now int64) []map[string]interface{} {
	var (
		event  map[string]interface{}
		events = make([]map[string]interface{}, 0)
	)

	_15 := now - 15
	_10 := now - 10
	_5 := now - 5

	event = make(map[string]interface{})
	event["@timestamp"] = time.Unix(_15, 0)
	event["host"] = "localhost"
	event["request_statusCode"] = "200"
	event["responseTime"] = 10.1
	events = append(events, event)

	event = make(map[string]interface{})
	event["@timestamp"] = time.Unix(_10, 0)
	event["host"] = "localhost"
	event["request_statusCode"] = "301"
	event["responseTime"] = 0.1
	events = append(events, event)

	event = make(map[string]interface{})
	event["@timestamp"] = time.Unix(_10, 0)
	event["host"] = "localhost"
	event["request_statusCode"] = "200"
	event["responseTime"] = 10.1
	events = append(events, event)

	event = make(map[string]interface{})
	event["@timestamp"] = time.Unix(_10, 0)
	event["host"] = "localhost"
	event["request_statusCode"] = "200"
	event["responseTime"] = 10.3
	events = append(events, event)

	event = make(map[string]interface{})
	event["@timestamp"] = time.Unix(_10, 0)
	event["host"] = "remote"
	event["request_statusCode"] = "200"
	event["responseTime"] = 10.1
	events = append(events, event)

	event = make(map[string]interface{})
	event["@timestamp"] = time.Unix(_5, 0)
	event["host"] = "localhost"
	event["request_statusCode"] = "200"
	event["responseTime"] = 10.1
	events = append(events, event)

	event = make(map[string]interface{})
	event["@timestamp"] = time.Unix(now, 0)
	event["host"] = "localhost"
	event["request_statusCode"] = "200"
	event["responseTime"] = 10.1
	events = append(events, event)
	return events
}

func TestLinkMetricFilter(t *testing.T) {
	var (
		config            map[interface{}]interface{}
		ok                bool
		batchWindow       = 5
		reserveWindow     = 20
		windowOffset      = 0
		ts                int64
		dropOriginalEvent = true
	)

	config = make(map[interface{}]interface{})
	config["fieldsLink"] = "host->request_statusCode"
	config["reserveWindow"] = reserveWindow
	config["batchWindow"] = batchWindow
	config["windowOffset"] = windowOffset
	config["drop_original_event"] = dropOriginalEvent

	f := (BuildFilter("LinkMetric", config)).(*LinkMetricFilter)
	f.SetBelongTo(topology.NewFilterBox(config))

	now := time.Now().Unix()
	for _, event := range createLinkMetricEvents(now) {
		f.Filter(event)
	}

	t.Logf("metric: %v", f.metric)
	//b, _ := json.Marshal(f.metric)
	//t.Logf("metric: %s", b)

	if ok == true {
		t.Error("LinkStatsMetric filter process should return false")
	}

	if len(f.metric) != 4 {
		t.Errorf("metric length should be 4")
	}
	_15 := now - 15
	ts = _15 - _15%(int64)(batchWindow)
	if f.metric[ts] == nil {
		t.Errorf("_15 should be in metric")
	}
	_5 := now - 5
	ts = _5 - _5%(int64)(batchWindow)
	if f.metric[ts] == nil {
		t.Errorf("_5 should be in metric")
	}
	_0 := now
	ts = _0 - _0%(int64)(batchWindow)
	if f.metric[ts] == nil {
		t.Errorf("_0 should be in metric")
	}

	_10 := now - 10
	ts = _10 - _10%(int64)(batchWindow)
	metric := f.metric[ts].(map[interface{}]interface{})
	if len(metric) != 2 {
		t.Errorf("_10 metric length should be 2")
	}
	if metric["localhost"] == nil {
		t.Errorf("localhost should be in _10 metric")
	}
	if metric["remote"] == nil {
		t.Errorf("remote should be in _10 metric")
	}

	localhostMetric := f.metric[ts].(map[interface{}]interface{})["localhost"].(map[interface{}]interface{})
	if len(localhostMetric) != 2 {
		t.Errorf("localhost metric length should be 2")
	}
	if localhostMetric["200"] == nil {
		t.Errorf("200 should be in localhost_metric")
	}
	if localhostMetric["301"] == nil {
		t.Errorf("301 should be in localhost_metric")
	}

	if localhostMetric["200"].(int) != 2 {
		t.Errorf("localhost->200 should be 2")
	}

	f.swapMetricMetricateMit()
	t.Logf("metricToEmit %v", f.metricToEmit)
	//b, _ := json.Marshal(f.metricToEmit)
	//t.Logf("metricToEmit: %s", b)

	if len(f.metricToEmit) != 4 {
		t.Errorf("metricToEmit length should be 4")
	}

	_15 = now - 15
	ts = _15 - _15%(int64)(batchWindow)
	if f.metricToEmit[ts] == nil {
		t.Errorf("_15 should be in metricToEmit")
	}
	_5 = now - 5
	ts = _5 - _5%(int64)(batchWindow)
	if f.metricToEmit[ts] == nil {
		t.Errorf("_5 should be in metricToEmit")
	}
	_0 = now
	ts = _0 - _0%(int64)(batchWindow)
	if f.metricToEmit[ts] == nil {
		t.Errorf("_0 should be in metricToEmit")
	}

	_10 = now - 10
	ts = _10 - _10%(int64)(batchWindow)
	metric = f.metricToEmit[ts].(map[interface{}]interface{})
	if len(metric) != 2 {
		t.Errorf("_10 metric length should be 2")
	}
	if metric["localhost"] == nil {
		t.Errorf("localhost should be in _10 metric")
	}
	if metric["remote"] == nil {
		t.Errorf("remote should be in _10 metric")
	}

	localhostMetric = f.metricToEmit[ts].(map[interface{}]interface{})["localhost"].(map[interface{}]interface{})
	if len(localhostMetric) != 2 {
		t.Errorf("localhost metric length should be 2")
	}
	if localhostMetric["200"] == nil {
		t.Errorf("200 should be in localhost_metric")
	}
	if localhostMetric["301"] == nil {
		t.Errorf("301 should be in localhost_metric")
	}

	if localhostMetric["200"].(int) != 2 {
		t.Errorf("localhost->200 should be 2")
	}
}

func TestLinkMetricFilterWindowOffset(t *testing.T) {
	var (
		config            map[interface{}]interface{}
		ok                bool
		batchWindow       = 5
		reserveWindow     = 20
		windowOffset      = 2
		ts                int64
		dropOriginalEvent = true
	)

	config = make(map[interface{}]interface{})
	config["fieldsLink"] = "host->request_statusCode"
	config["reserveWindow"] = reserveWindow
	config["batchWindow"] = batchWindow
	config["windowOffset"] = windowOffset
	config["drop_original_event"] = dropOriginalEvent

	f := (BuildFilter("LinkMetric", config)).(*LinkMetricFilter)
	f.SetBelongTo(topology.NewFilterBox(config))

	now := time.Now().Unix()
	for _, event := range createLinkMetricEvents(now) {
		f.Filter(event)
	}

	t.Logf("metric: %v", f.metric)
	//b, _ := json.Marshal(f.metric)
	//t.Logf("metric: %s", b)

	if ok == true {
		t.Error("LinkStatsMetric filter process should return false")
	}

	if len(f.metric) != 4 {
		t.Errorf("%v", f.metricToEmit[ts])
	}
	_15 := now - 15
	ts = _15 - _15%(int64)(batchWindow)
	if f.metric[ts] == nil {
		t.Errorf("_15 should be in metric")
	}
	_5 := now - 5
	ts = _5 - _5%(int64)(batchWindow)
	if f.metric[ts] == nil {
		t.Errorf("_5 should be in metric")
	}
	_0 := now
	ts = _0 - _0%(int64)(batchWindow)
	if f.metric[ts] == nil {
		t.Errorf("_0 should be in metric")
	}

	_10 := now - 10
	ts = _10 - _10%(int64)(batchWindow)
	metric := f.metric[ts].(map[interface{}]interface{})
	t.Logf("metric: %v", metric)
	if len(metric) != 2 {
		t.Errorf("_10 metric length should be 2")
	}
	if metric["localhost"] == nil {
		t.Errorf("localhost should be in _10 metric")
	}
	if metric["remote"] == nil {
		t.Errorf("remote should be in _10 metric")
	}

	localhostMetric := f.metric[ts].(map[interface{}]interface{})["localhost"].(map[interface{}]interface{})
	if len(localhostMetric) != 2 {
		t.Errorf("localhost metric length should be 2")
	}
	if localhostMetric["200"] == nil {
		t.Errorf("200 should be in localhost_metric")
	}
	if localhostMetric["301"] == nil {
		t.Errorf("301 should be in localhost_metric")
	}
	if localhostMetric["200"].(int) != 2 {
		t.Errorf("localhost->200 should be 2")
	}

	f.swapMetricMetricateMit()
	t.Logf("metricToEmit %v", f.metricToEmit)
	if len(f.metricToEmit) != 2 {
		t.Error(f.metricToEmit)
	}

	_15 = now - 15
	ts = _15 - _15%(int64)(batchWindow)
	if f.metricToEmit[ts] == nil {
		t.Errorf("_15 should be in metricToEmit")
	}
	_5 = now - 5
	ts = _5 - _5%(int64)(batchWindow)
	if f.metricToEmit[ts] != nil {
		t.Errorf("_5 should not be in metricToEmit")
	}
	_0 = now
	ts = _0 - _0%(int64)(batchWindow)
	if f.metricToEmit[ts] != nil {
		t.Errorf("_0 should not be in metricToEmit")
	}

	_10 = now - 10
	ts = _10 - _10%(int64)(batchWindow)
	metric = f.metricToEmit[ts].(map[interface{}]interface{})
	if len(metric) != 2 {
		t.Errorf("_10 metric length should be 2")
	}
	if metric["localhost"] == nil {
		t.Errorf("localhost should be in _10 metric")
	}
	if metric["remote"] == nil {
		t.Errorf("remote should be in _10 metric")
	}

	localhostMetric = f.metricToEmit[ts].(map[interface{}]interface{})["localhost"].(map[interface{}]interface{})
	if len(localhostMetric) != 2 {
		t.Errorf("_10->localhost metric length should be 2")
	}
	if localhostMetric["200"].(int) != 2 {
		t.Errorf("_10->localhost->200 shoule be 2")
	}
	if localhostMetric["301"].(int) != 1 {
		t.Errorf("_10->localhost->301 shoule be 1")
	}
}

func _testLinkMetricFilterAccumulateMode(t *testing.T, f *LinkMetricFilter, now int64, count200, count301 int) {
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
	t.Logf("metric: %v", metric)
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
		t.Errorf("_10->localhost metric length should be 2")
	}
	if localhostMetric["200"].(int) != count200 {
		t.Errorf("_10->localhost->200 should be %d", count200)
	}
	if localhostMetric["301"].(int) != count301 {
		t.Errorf("_10->localhost->301 should be %d", count301)
	}
}

func TestLinkMetricFilterCumulativeMode(t *testing.T) {
	var (
		config            map[interface{}]interface{}
		f                 *LinkMetricFilter
		batchWindow                   = 5
		reserveWindow                 = 20
		windowOffset                  = 0
		accumulateMode    interface{} = "cumulative"
		dropOriginalEvent             = true
	)

	config = make(map[interface{}]interface{})
	config["fieldsLink"] = "host->request_statusCode"
	config["reserveWindow"] = reserveWindow
	config["batchWindow"] = batchWindow
	config["windowOffset"] = windowOffset
	config["accumulateMode"] = accumulateMode
	config["drop_original_event"] = dropOriginalEvent

	f = (BuildFilter("LinkMetric", config)).(*LinkMetricFilter)
	f.SetBelongTo(topology.NewFilterBox(config))

	now := time.Now().Unix()
	for _, event := range createLinkMetricEvents(now) {
		f.Filter(event)
	}
	f.swapMetricMetricateMit()
	for _, event := range createLinkMetricEvents(now) {
		f.Filter(event)
	}

	f.swapMetricMetricateMit()
	t.Logf("metricToEmit: %v", f.metricToEmit)

	_testLinkMetricFilterAccumulateMode(t, f, now, 4, 2)
}

func TestLinkMetricFilterSeparateMode(t *testing.T) {
	var (
		config            map[interface{}]interface{}
		f                 *LinkMetricFilter
		batchWindow                   = 5
		reserveWindow                 = 20
		windowOffset                  = 0
		accumulateMode    interface{} = "separate"
		dropOriginalEvent             = true
	)

	config = make(map[interface{}]interface{})
	config["fieldsLink"] = "host->request_statusCode"
	config["reserveWindow"] = reserveWindow
	config["batchWindow"] = batchWindow
	config["windowOffset"] = windowOffset
	config["accumulateMode"] = accumulateMode
	config["drop_original_event"] = dropOriginalEvent

	f = (BuildFilter("LinkMetric", config)).(*LinkMetricFilter)
	f.SetBelongTo(topology.NewFilterBox(config))

	now := time.Now().Unix()
	for _, event := range createLinkMetricEvents(now) {
		f.Filter(event)
	}
	f.swapMetricMetricateMit()
	for _, event := range createLinkMetricEvents(now) {
		f.Filter(event)
	}

	f.swapMetricMetricateMit()
	t.Logf("metricToEmit: %v", f.metricToEmit)
	if len(f.metricToEmit) != 4 {
		t.Error(f.metricToEmit)
	}

	_testLinkMetricFilterAccumulateMode(t, f, now, 2, 1)
}
