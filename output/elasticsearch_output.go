package output

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/kevinu2/gohangout/codec"
	"github.com/kevinu2/gohangout/condition_filter"
	"github.com/kevinu2/gohangout/topology"
	"github.com/kevinu2/gohangout/value_render"
)

const (
	defaultIndexType = "logs"
	defaultEsVersion = 6
	defaultAction    = "index"
)

var (
	f                 func() codec.Encoder
	defaultNormalResp = []byte(`"errors":false,`)
)

type Action struct {
	op        string
	index     string
	indexType string
	id        string
	routing   string
	event     map[string]interface{}
	rawSource []byte
	esVersion int
}

func (action *Action) Encode() []byte {
	var (
		meta = make([]byte, 0, 1000)
		buf  []byte
		err  error
	)
	meta = append(meta, `{"`+action.op+`":{"_index":`...)
	index, _ := f().Encode(action.index)
	meta = append(meta, index...)

	if action.esVersion <= defaultEsVersion {
		meta = append(meta, `,"_type":`...)
		indexType, _ := f().Encode(action.indexType)
		meta = append(meta, indexType...)
	}

	if action.id != "" {
		meta = append(meta, `,"_id":`...)
		docId, _ := f().Encode(action.id)
		meta = append(meta, docId...)
	}

	meta = append(meta, `,"routing":`...)
	routing, _ := f().Encode(action.routing)
	meta = append(meta, routing...)

	meta = append(meta, "}}\n"...)

	if action.rawSource == nil {
		buf, err = f().Encode(action.event)
		if err != nil {
			glog.Errorf("could marshal event(%v):%s", action.event, err)
			return nil
		}
	} else {
		buf = action.rawSource
	}

	bulkBuf := make([]byte, 0, len(meta)+len(buf)+1)
	bulkBuf = append(bulkBuf, meta...)
	bulkBuf = append(bulkBuf, buf[:]...)
	bulkBuf = append(bulkBuf, '\n')
	return bulkBuf
}

type ESBulkRequest struct {
	events  []Event
	bulkBuf []byte
}

func (br *ESBulkRequest) add(event Event) {
	br.bulkBuf = append(br.bulkBuf, event.Encode()...)
	br.events = append(br.events, event)
}

func (br *ESBulkRequest) bufSizeByte() int {
	return len(br.bulkBuf)
}
func (br *ESBulkRequest) eventCount() int {
	return len(br.events)
}
func (br *ESBulkRequest) readBuf() []byte {
	return br.bulkBuf
}

type ElasticsearchOutput struct {
	config map[interface{}]interface{}

	action           string
	index            value_render.ValueRender
	indexType        value_render.ValueRender
	id               value_render.ValueRender
	routing          value_render.ValueRender
	sourceField      value_render.ValueRender
	bytesSourceField value_render.ValueRender
	esVersion        int
	bulkProcessor    BulkProcessor

	hosts    []string
	user     string
	password string
}

func esGetRetryEvents(resp *http.Response, respBody []byte, bulkRequest *BulkRequest) ([]int, []int, BulkRequest) {
	retry := make([]int, 0)
	noRetry := make([]int, 0)
	//make a string index to avoid json decode for speed up over 90%+ sciences
	if bytes.Index(respBody, defaultNormalResp) != -1 {
		return retry, noRetry, nil
	}
	var responseI interface{}
	err := json.Unmarshal(respBody, &responseI)
	if err != nil {
		glog.Errorf(`could not unmarshal bulk response:"%s". will NOT retry. %s`, err, string(respBody[:100]))
		return retry, noRetry, nil
	}

	bulkResponse := responseI.(map[string]interface{})
	glog.V(20).Infof("%v", bulkResponse)

	if bulkResponse["errors"] == nil {
		glog.Infof("could NOT get errors in response:%s", string(respBody))
		return retry, noRetry, nil
	}

	if bulkResponse["errors"].(bool) == false {
		return retry, noRetry, nil
	}

	hasLog := false
	for i, item := range bulkResponse["items"].([]interface{}) {
		index := item.(map[string]interface{})["index"].(map[string]interface{})

		if errorValue, ok := index["error"]; ok {
			//errorType := errorValue.(map[string]interface{})["type"].(string)
			if !hasLog {
				glog.Infof("error :%v", errorValue)
				hasLog = true
			}

			status := index["status"].(float64)
			if status == 429 || status >= 500 {
				retry = append(retry, i)
			} else {
				noRetry = append(noRetry, i)
			}
		}
	}
	newBulkRequest := buildRetryBulkRequest(retry, noRetry, bulkRequest)
	return retry, noRetry, newBulkRequest
}

func buildRetryBulkRequest(shouldRetry, noRetry []int, bulkRequest *BulkRequest) BulkRequest {
	esBulkRequest := (*bulkRequest).(*ESBulkRequest)
	if len(noRetry) > 0 {
		b, err := json.Marshal(esBulkRequest.events[noRetry[0]].(*Action).event)
		if err != nil {
			glog.Infof("one failed doc that need no retry: %+v", esBulkRequest.events[noRetry[0]].(*Action).event)
		} else {
			glog.Infof("one failed doc that need no retry: %s", b)
		}
	}

	if len(shouldRetry) > 0 {
		newBulkRequest := &ESBulkRequest{
			bulkBuf: make([]byte, 0),
		}
		for _, i := range shouldRetry {
			newBulkRequest.add(esBulkRequest.events[i])
		}
		return newBulkRequest
	}
	return nil
}

func init() {
	Register("Elasticsearch", newElasticsearchOutput)
}

func newElasticsearchOutput(config map[interface{}]interface{}) topology.Output {
	rst := &ElasticsearchOutput{
		config: config,
	}

	_codec := "simplejson"
	if v, ok := config["codec"]; ok {
		_codec = v.(string)
	}
	f = func() codec.Encoder { return codec.NewEncoder(_codec) }

	if v, ok := config["action"]; ok {
		rst.action = v.(string)
	} else {
		rst.action = defaultAction
	}

	if v, ok := config["index"]; ok {
		rst.index = value_render.GetValueRender(v.(string))
	} else {
		glog.Fatal("index must be set in elasticsearch output")
	}

	if v, ok := config["index_time_location"]; ok {
		if e, ok := rst.index.(*value_render.IndexRender); ok {
			e.SetTimeLocation(v.(string))
		} else {
			glog.Fatal("index_time_location is not supported in this index format")
		}
	}

	if v, ok := config["index_type"]; ok {
		rst.indexType = value_render.GetValueRender(v.(string))
	} else {
		rst.indexType = value_render.GetValueRender(defaultIndexType)
	}

	if v, ok := config["id"]; ok {
		rst.id = value_render.GetValueRender(v.(string))
	} else {
		rst.id = nil
	}

	if v, ok := config["routing"]; ok {
		rst.routing = value_render.GetValueRender(v.(string))
	} else {
		rst.routing = nil
	}

	if v, ok := config["source_field"]; ok {
		rst.sourceField = value_render.GetValueRender2(v.(string))
	} else {
		rst.sourceField = nil
	}

	if v, ok := config["bytes_source_field"]; ok {
		rst.bytesSourceField = value_render.GetValueRender2(v.(string))
	} else {
		rst.bytesSourceField = nil
	}

	if v, ok := config["es_version"]; ok {
		rst.esVersion = v.(int)
	} else {
		rst.esVersion = defaultEsVersion
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

	var headers = map[string]string{"Content-Type": "application/x-ndjson"}
	if v, ok := config["headers"]; ok {
		for keyI, valueI := range v.(map[interface{}]interface{}) {
			headers[keyI.(string)] = valueI.(string)
		}
	}
	var requestMethod = "POST"

	var retryResponseCode = make(map[int]bool)
	if v, ok := config["retry_response_code"]; ok {
		for _, cI := range v.([]interface{}) {
			retryResponseCode[cI.(int)] = true
		}
	} else {
		retryResponseCode[401] = true
		retryResponseCode[502] = true
	}

	byteSizeAppliedInAdvance := bulkSize + 1024*1024
	if byteSizeAppliedInAdvance > MaxByteSizeAppliedInAdvance {
		byteSizeAppliedInAdvance = MaxByteSizeAppliedInAdvance
	}
	var f = func() BulkRequest {
		return &ESBulkRequest{
			bulkBuf: make([]byte, 0, byteSizeAppliedInAdvance),
		}
	}

	var hosts = make([]string, 0)
	if v, ok := config["hosts"]; ok {
		for _, h := range v.([]interface{}) {
			user, password, host := getUserPasswordAndHost(h.(string))
			if host == "" {
				glog.Fatalf("invalid host: %q", host)
			}
			rst.user = user
			rst.password = password
			hosts = append(hosts, host)
		}
	} else {
		glog.Fatal("hosts must be set in elasticsearch output")
	}
	rst.hosts = hosts

	var err error
	if sniff, ok := config["sniff"]; ok {
		glog.Infof("sniff hosts in es cluster")
		sniff := sniff.(map[interface{}]interface{})
		hosts, err = sniffNodes(config)
		glog.Infof("new hosts after sniff: %v", hosts)
		if err != nil {
			glog.Fatalf("could not sniff hosts: %v", err)
		}
		if len(hosts) == 0 {
			glog.Fatal("no available hosts after sniff")
		}
		rst.hosts = hosts

		refreshInterval := sniff["refresh_interval"].(int)
		if refreshInterval > 0 {
			go func() {
				for range time.NewTicker(time.Second * time.Duration(refreshInterval)).C {
					hosts, err = sniffNodes(config)
					if err != nil {
						glog.Errorf("could not sniff hosts: %v", err)
					} else {
						if !reflect.DeepEqual(rst.hosts, hosts) {
							glog.Infof("new hosts after sniff: %v", hosts)
							rst.hosts = hosts
							rst.bulkProcessor.(*HTTPBulkProcessor).resetHosts(rst.assembleHosts())
						}
					}
				}
			}()
		}
	}
	rst.bulkProcessor = NewHTTPBulkProcessor(headers, rst.assembleHosts(), requestMethod, retryResponseCode, bulkSize, bulkActions, flushInterval, concurrent, compress, f, esGetRetryEvents)
	return rst
}

func getUserPasswordAndHost(url string) (user, password, host string) {
	p := regexp.MustCompile(`^(?i)(?:http(?:s?)://)(?:([^:]+):([^@]+)@)?(\S+)$`)
	r := p.FindStringSubmatch(url)
	if len(r) == 4 {
		user = r[1]
		password = r[2]
		host = strings.TrimRight(r[3], "/")
		return
	} else if len(r) == 2 {
		host = strings.TrimRight(r[1], "/")
		return
	} else {
		glog.Infof("%q is invalid host format", host)
		return
	}
}

func sniffNodes(config map[interface{}]interface{}) ([]string, error) {
	sniff := config["sniff"].(map[interface{}]interface{})
	var (
		match string
		ok    bool
	)
	v, _ := sniff["match"]
	if v != nil {
		match, ok = v.(string)
		if !ok {
			glog.Fatal("match in sniff settings must be string")
		}
	}
	for _, host := range config["hosts"].([]interface{}) {
		host := host.(string)
		if nodes, err := sniffNodesFromOneHost(host, match); err == nil {
			return nodes, err
		} else {
			glog.Errorf("sniff nodes error from %s: %v", RemoveHttpAuthRegexp.ReplaceAllString(host, "${1}"), err)
		}
	}
	return nil, errors.New("sniff nodes error from all hosts")
}

func sniffNodesFromOneHost(host string, match string) ([]string, error) {
	url := strings.TrimRight(host, "/") + "/_nodes/_all/http"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s:%d", url, resp.StatusCode)
	}

	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	v := make(map[string]interface{})
	err = json.Unmarshal(respBody, &v)
	if err != nil {
		return nil, err
	}

	return filterNodesIPList(v, match)
}

// filterNodesIPList gets ip lists from what is returned from _nodes/_all/info
// it uses `match` config to filter the nodes you what
func filterNodesIPList(v map[string]interface{}, match string) ([]string, error) {
	if len(match) > 0 {
		f := condition_filter.NewCondition(match)
		IPList := make([]string, 0)
		for _, info := range v["nodes"].(map[string]interface{}) {
			if f.Pass(info.(map[string]interface{})) {
				info := info.(map[string]interface{})
				publishAddress := (info["http"].(map[string]interface{}))["publish_address"].(string)
				IPList = append(IPList, publishAddress)
			}
		}
		return IPList, nil
	}

	IPList := make([]string, 0)
	for _, info := range v["nodes"].([]interface{}) {
		info := info.(map[string]interface{})
		publishAddress := (info["http"].(map[string]interface{}))["publish_address"].(string)
		IPList = append(IPList, publishAddress)
	}
	return IPList, nil
}

// create ES host list using user, password and hosts
func (p *ElasticsearchOutput) assembleHosts() (hosts []string) {
	hosts = make([]string, 0)
	for _, host := range p.hosts {
		if len(p.user) > 0 {
			hosts = append(hosts, fmt.Sprintf("http://%s:%s@%s", p.user, p.password, host))
		} else {
			hosts = append(hosts, fmt.Sprintf("http://%s", host))
		}
	}
	return
}

// Emit adds the event to bulkProcessor
func (p *ElasticsearchOutput) Emit(event map[string]interface{}) {
	var (
		index     = p.index.Render(event).(string)
		indexType = p.indexType.Render(event).(string)
		op        = p.action
		esVersion = p.esVersion
		id        string
		routing   string
	)
	if p.id == nil {
		id = ""
	} else {
		t := p.id.Render(event)
		if t == nil {
			id = ""
			glog.V(20).Infof("could not render id:%s", event)
		} else {
			id = t.(string)
		}
	}

	if p.routing == nil {
		routing = ""
	} else {
		t := p.routing.Render(event)
		if t == nil {
			routing = ""
			glog.V(20).Infof("could not render routing:%s", event)
		} else {
			routing = t.(string)
		}
	}

	if p.sourceField == nil && p.bytesSourceField == nil {
		p.bulkProcessor.add(&Action{op, index, indexType, id, routing, event, nil, esVersion})
	} else if p.bytesSourceField != nil {
		t := p.bytesSourceField.Render(event)
		if t == nil {
			p.bulkProcessor.add(&Action{op, index, indexType, id, routing, event, nil, esVersion})
		} else {
			p.bulkProcessor.add(&Action{op, index, indexType, id, routing, event, t.([]byte), esVersion})
		}
	} else {
		t := p.sourceField.Render(event)
		if t == nil {
			p.bulkProcessor.add(&Action{op, index, indexType, id, routing, event, nil, esVersion})
		} else {
			p.bulkProcessor.add(&Action{op, index, indexType, id, routing, event, []byte(t.(string)), esVersion})
		}
	}
}

func (p *ElasticsearchOutput) Shutdown() {
	p.bulkProcessor.awaitclose(30 * time.Second)
}
