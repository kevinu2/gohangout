package filter

import (
	"reflect"

	"github.com/childe/gohangout/condition_filter"
	"github.com/childe/gohangout/field_setter"
	"github.com/childe/gohangout/value_render"
	"github.com/golang-collections/collections/stack"
	"github.com/golang/glog"
)

type Filter interface {
	Pass(map[string]interface{}) bool
	Process(map[string]interface{}) (map[string]interface{}, bool)
	PostProcess(map[string]interface{}, bool) map[string]interface{}
	EmitExtraEvents(*stack.Stack) []map[string]interface{}
}

func GetFilter(filterType string, config map[interface{}]interface{}) Filter {
	switch filterType {
	case "Add":
		return NewAddFilter(config)
	case "Grok":
		return NewGrokFilter(config)
	case "Date":
		return NewDateFilter(config)
	case "Drop":
		return NewDropFilter(config)
	case "Json":
		return NewJsonFilter(config)
	case "LinkMetric":
		return NewLinkMetricFilter(config)
	}
	glog.Fatalf("could build %s filter plugin", filterType)
	return nil
}

type BaseFilter struct {
	config          map[interface{}]interface{}
	conditionFilter *condition_filter.ConditionFilter

	failTag      string
	removeFields []string
	addFields    map[field_setter.FieldSetter]value_render.ValueRender
}

func NewBaseFilter(config map[interface{}]interface{}) BaseFilter {
	f := BaseFilter{
		config:          config,
		conditionFilter: condition_filter.NewConditionFilter(config),
	}
	if v, ok := config["failTag"]; ok {
		f.failTag = v.(string)
	} else {
		f.failTag = ""
	}

	if remove_fields, ok := config["remove_fields"]; ok {
		f.removeFields = make([]string, 0)
		for _, field := range remove_fields.([]interface{}) {
			f.removeFields = append(f.removeFields, field.(string))
		}
	} else {
		f.removeFields = nil
	}

	if add_fields, ok := config["add_fields"]; ok {
		f.addFields = make(map[field_setter.FieldSetter]value_render.ValueRender)
		for k, v := range add_fields.(map[interface{}]interface{}) {
			fieldSetter := field_setter.NewFieldSetter(k.(string))
			if fieldSetter == nil {
				glog.Fatalf("could build field setter from %s", k.(string))
			}
			f.addFields[fieldSetter] = value_render.GetValueRender(v.(string))
		}
	} else {
		f.addFields = nil
	}
	return f
}

func (f *BaseFilter) Pass(event map[string]interface{}) bool {
	return f.conditionFilter.Pass(event)
}

func (f *BaseFilter) Process(event map[string]interface{}) (map[string]interface{}, bool) {
	return event, true
}
func (f *BaseFilter) EmitExtraEvents(*stack.Stack) []map[string]interface{} {
	return nil
}
func (f *BaseFilter) PostProcess(event map[string]interface{}, success bool) map[string]interface{} {
	if success {
		if f.removeFields != nil {
			for _, field := range f.removeFields {
				delete(event, field)
			}
		}
		for fs, v := range f.addFields {
			event = fs.SetField(event, v.Render(event), "", false)
		}
	} else {
		if f.failTag != "" {
			if tags, ok := event["tags"]; ok {
				if reflect.TypeOf(tags).Kind() == reflect.String {
					event["tags"] = []string{tags.(string), f.failTag}
				} else if reflect.TypeOf(tags).Kind() == reflect.Array {
					event["tags"] = append(tags.([]interface{}), f.failTag)
				} else {
				}
			} else {
				event["tags"] = f.failTag
			}
		}
	}
	return event
}
