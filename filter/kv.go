package filter

import (
	"strings"

	"github.com/golang/glog"
	"github.com/kevinu2/gohangout/field_setter"
	"github.com/kevinu2/gohangout/topology"
	"github.com/kevinu2/gohangout/value_render"
)

type KVFilter struct {
	config      map[interface{}]interface{}
	fields      map[field_setter.FieldSetter]value_render.ValueRender
	src         value_render.ValueRender
	target      string
	fieldSplit  string
	valueSplit  string
	trim        string
	trimKey     string
	includeKeys map[string]bool
	excludeKeys map[string]bool
}

func init() {
	Register("KV", newKVFilter)
}

func newKVFilter(config map[interface{}]interface{}) topology.Filter {
	plugin := &KVFilter{
		config: config,
		fields: make(map[field_setter.FieldSetter]value_render.ValueRender),
	}

	if src, ok := config["src"]; ok {
		plugin.src = value_render.GetValueRender2(src.(string))
	} else {
		glog.Fatal("src must be set in kv filter")
	}

	if target, ok := config["target"]; ok {
		plugin.target = target.(string)
	} else {
		plugin.target = ""
	}

	if fieldSplit, ok := config["field_split"]; ok {
		plugin.fieldSplit = fieldSplit.(string)
	} else {
		glog.Fatal("field_split must be set in kv filter")
	}

	if valueSplit, ok := config["value_split"]; ok {
		plugin.valueSplit = valueSplit.(string)
	} else {
		glog.Fatal("value_split must be set in kv filter")
	}

	if trim, ok := config["trim"]; ok {
		plugin.trim = trim.(string)
	} else {
		plugin.trim = ""
	}

	if trimKey, ok := config["trim_key"]; ok {
		plugin.trimKey = trimKey.(string)
	} else {
		plugin.trimKey = ""
	}

	plugin.includeKeys = make(map[string]bool)
	if includeKeys, ok := config["include_keys"]; ok {
		for _, k := range includeKeys.([]interface{}) {
			plugin.includeKeys[k.(string)] = true
		}
	}

	plugin.excludeKeys = make(map[string]bool)
	if excludeKeys, ok := config["exclude_keys"]; ok {
		for _, k := range excludeKeys.([]interface{}) {
			plugin.excludeKeys[k.(string)] = true
		}
	}

	return plugin
}

func (p *KVFilter) Filter(event map[string]interface{}) (map[string]interface{}, bool) {
	msg := p.src.Render(event)
	if msg == nil {
		return event, false
	}
	A := strings.Split(msg.(string), p.fieldSplit)
	if len(A) == 1 {
		return event, false
	}

	var o = event
	if p.target != "" {
		o = make(map[string]interface{})
		event[p.target] = o
	}

	var success = true
	var key string
	for _, kv := range A {
		a := strings.SplitN(kv, p.valueSplit, 2)
		if len(a) != 2 {
			success = false
			continue
		}

		key = strings.Trim(a[0], p.trimKey)

		if _, ok := p.excludeKeys[key]; ok {
			continue
		}

		if _, ok := p.includeKeys[key]; len(p.includeKeys) == 0 || ok {
			o[key] = strings.Trim(a[1], p.trim)
		}
	}
	return event, success
}
