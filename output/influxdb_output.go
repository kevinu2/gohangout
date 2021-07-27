package output

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/kevinu2/gohangout/topology"
	"github.com/kevinu2/gohangout/value_render"
)

type InAction struct {
	measurement string
	event       map[string]interface{}
	tags        []string
	fields      []string
	timestamp   string
}

func (action *InAction) Encode() []byte {
	bulkBuf := []byte(action.measurement)

	//tag set
	tagSet := make([]string, 0)
	for _, tag := range action.tags {
		if v, ok := action.event[tag]; ok {
			tagSet = append(tagSet, fmt.Sprintf("%s=%v", tag, v))
		}
	}
	if len(tagSet) > 0 {
		bulkBuf = append(bulkBuf, ',')
		bulkBuf = append(bulkBuf, strings.Join(tagSet, ",")...)
	}

	//field set
	fieldSet := make([]string, 0)
	for _, field := range action.fields {
		if v, ok := action.event[field]; ok {
			fieldSet = append(fieldSet, fmt.Sprintf("%s=%v", field, v))
		}
	}
	if len(fieldSet) <= 0 {
		glog.V(20).Infof("field set is nil. fields: %v. event: %v", action.fields, action.event)
		return nil
	} else {
		bulkBuf = append(bulkBuf, ' ')
		bulkBuf = append(bulkBuf, strings.Join(fieldSet, ",")...)
	}

	//timestamp
	t := action.event[action.timestamp]
	if t != nil && reflect.TypeOf(t).String() == "time.Time" {
		bulkBuf = append(bulkBuf, fmt.Sprintf(" %d", t.(time.Time).UnixNano())...)
	} else {
		glog.V(20).Infof("%s is not time.Time", action.timestamp)
	}

	return bulkBuf
}

type InfluxdbBulkRequest struct {
	events  []Event
	bulkBuf []byte
}

func (br *InfluxdbBulkRequest) add(event Event) {
	br.bulkBuf = append(br.bulkBuf, event.Encode()...)
	br.bulkBuf = append(br.bulkBuf, '\n')
	br.events = append(br.events, event)
}

func (br *InfluxdbBulkRequest) bufSizeByte() int {
	return len(br.bulkBuf)
}
func (br *InfluxdbBulkRequest) eventCount() int {
	return len(br.events)
}
func (br *InfluxdbBulkRequest) readBuf() []byte {
	return br.bulkBuf
}

type InfluxdbOutput struct {
	config map[interface{}]interface{}

	db          string
	measurement value_render.ValueRender
	tags        []string
	fields      []string
	timestamp   string

	bulkProcessor BulkProcessor
}

func influxdbGetRetryEvents(resp *http.Response, respBody []byte, bulkRequest *BulkRequest) ([]int, []int, BulkRequest) {
	return nil, nil, nil
}

func init() {
	Register("Influxdb", newInfluxdbOutput)
}

func newInfluxdbOutput(config map[interface{}]interface{}) topology.Output {
	rst := &InfluxdbOutput{
		config: config,
	}

	if v, ok := config["db"]; ok {
		rst.db = v.(string)
	} else {
		glog.Fatal("db must be set in elasticsearch output")
	}

	if v, ok := config["measurement"]; ok {
		rst.measurement = value_render.GetValueRender(v.(string))
	} else {
		glog.Fatal("measurement must be set in elasticsearch output")
	}

	if v, ok := config["tags"]; ok {
		for _, t := range v.([]interface{}) {
			rst.tags = append(rst.tags, t.(string))
		}
	}
	if v, ok := config["fields"]; ok {
		for _, f := range v.([]interface{}) {
			rst.fields = append(rst.fields, f.(string))
		}
	}
	if v, ok := config["timestamp"]; ok {
		rst.timestamp = v.(string)
	} else {
		rst.timestamp = "@timestamp"
	}

	var (
		bulkSize, bulkActions, flushInterval, concurrent int
		compress                                         bool
	)
	if v, ok := config["bulk_size"]; ok {
		bulkSize = v.(int) * 1024 * 1024
	} else {
		bulkSize = DefaultBulkSize
	}

	if v, ok := config["bulk_actions"]; ok {
		bulkActions = v.(int)
	} else {
		bulkActions = DefaultBulkActions
	}
	if v, ok := config["flush_interval"]; ok {
		flushInterval = v.(int)
	} else {
		flushInterval = DefaultFlushInterval
	}
	if v, ok := config["concurrent"]; ok {
		concurrent = v.(int)
	} else {
		concurrent = DefaultConcurrent
	}
	if concurrent <= 0 {
		glog.Fatal("concurrent must > 0")
	}
	if v, ok := config["compress"]; ok {
		compress = v.(bool)
	} else {
		compress = true
	}

	var hosts []string
	if v, ok := config["hosts"]; ok {
		for _, h := range v.([]interface{}) {
			hosts = append(hosts, h.(string)+"/write?db="+rst.db)
		}
	} else {
		glog.Fatal("hosts must be set in elasticsearch output")
	}

	headers := make(map[string]string)
	if v, ok := config["headers"]; ok {
		for keyI, valueI := range v.(map[interface{}]interface{}) {
			headers[keyI.(string)] = valueI.(string)
		}
	}
	var requestMethod = "POST"

	var retryResponseCode map[int]bool
	if v, ok := config["retry_response_code"]; ok {
		for _, cI := range v.([]interface{}) {
			retryResponseCode[cI.(int)] = true
		}
	}

	byteSizeAppliedInAdvance := bulkSize + 1024*1024
	if byteSizeAppliedInAdvance > MaxByteSizeAppliedInAdvance {
		byteSizeAppliedInAdvance = MaxByteSizeAppliedInAdvance
	}
	var f = func() BulkRequest {
		return &InfluxdbBulkRequest{
			bulkBuf: make([]byte, 0, byteSizeAppliedInAdvance),
		}
	}

	rst.bulkProcessor = NewHTTPBulkProcessor(headers, hosts, requestMethod, retryResponseCode, bulkSize, bulkActions, flushInterval, concurrent, compress, f, influxdbGetRetryEvents)
	return rst
}

func (p *InfluxdbOutput) Emit(event map[string]interface{}) {
	var (
		measurement = p.measurement.Render(event).(string)
	)
	p.bulkProcessor.add(&InAction{measurement, event, p.tags, p.fields, p.timestamp})
}
func (p *InfluxdbOutput) Shutdown() {
	p.bulkProcessor.awaitclose(30 * time.Second)
}
