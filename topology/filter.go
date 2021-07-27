package topology

import (
	"reflect"

	"github.com/golang/glog"
	"github.com/kevinu2/gohangout/condition_filter"
	"github.com/kevinu2/gohangout/field_deleter"
	"github.com/kevinu2/gohangout/field_setter"
	"github.com/kevinu2/gohangout/value_render"
)

type Filter interface {
	Filter(map[string]interface{}) (map[string]interface{}, bool)
}

type FilterBox struct {
	Filter Filter

	conditionFilter *condition_filter.ConditionFilter

	config map[interface{}]interface{}

	failTag      string
	removeFields []field_deleter.FieldDeleter
	addFields    map[field_setter.FieldSetter]value_render.ValueRender
}

func NewFilterBox(config map[interface{}]interface{}) *FilterBox {
	f := FilterBox{
		config:          config,
		conditionFilter: condition_filter.NewConditionFilter(config),
	}

	if v, ok := config["failTag"]; ok {
		f.failTag = v.(string)
	} else {
		f.failTag = ""
	}

	if removeFields, ok := config["remove_fields"]; ok {
		f.removeFields = make([]field_deleter.FieldDeleter, 0)
		for _, field := range removeFields.([]interface{}) {
			f.removeFields = append(f.removeFields, field_deleter.NewFieldDeleter(field.(string)))
		}
	} else {
		f.removeFields = nil
	}

	if addFields, ok := config["add_fields"]; ok {
		f.addFields = make(map[field_setter.FieldSetter]value_render.ValueRender)
		for k, v := range addFields.(map[interface{}]interface{}) {
			fieldSetter := field_setter.NewFieldSetter(k.(string))
			if fieldSetter == nil {
				glog.Fatalf("could build field setter from %s", k.(string))
			}
			f.addFields[fieldSetter] = value_render.GetValueRender(v.(string))
		}
	} else {
		f.addFields = nil
	}
	return &f
}

func (f *FilterBox) PostProcess(event map[string]interface{}, success bool) map[string]interface{} {
	if success {
		for fs, v := range f.addFields {
			event = fs.SetField(event, v.Render(event), "", false)
		}
		if f.removeFields != nil {
			for _, d := range f.removeFields {
				d.Delete(event)
			}
		}
	} else {
		if f.failTag != "" {
			if tags, ok := event["tags"]; ok {
				if reflect.TypeOf(tags).Kind() == reflect.String {
					event["tags"] = []interface{}{tags.(string), f.failTag}
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

func (f *FilterBox) Process(event map[string]interface{}) map[string]interface{} {
	var rst bool

	if f.conditionFilter.Pass(event) {
		event, rst = f.Filter.Filter(event)
		if event == nil {
			return nil
		}
		event = f.PostProcess(event, rst)
	}
	return event
}

type buildFilterFunc func(filterType string, config map[interface{}]interface{}) Filter

func BuildFilterBoxes(config map[string]interface{}, buildFilter buildFilterFunc) []*FilterBox {
	if _, ok := config["filters"]; !ok {
		return nil
	}

	filtersI := config["filters"].([]interface{})
	filters := make([]Filter, len(filtersI))

	for i := 0; i < len(filters); i++ {
		for filterTypeI, filterConfigI := range filtersI[i].(map[interface{}]interface{}) {
			filterType := filterTypeI.(string)
			glog.Infof("filter type: %s", filterType)
			filterConfig := filterConfigI.(map[interface{}]interface{})
			glog.Infof("filter config: %v", filterConfig)

			filterPlugin := buildFilter(filterType, filterConfig)

			filters[i] = filterPlugin
		}
	}

	boxes := make([]*FilterBox, len(filters))
	for i := 0; i < len(filters); i++ {
		for _, cfg := range filtersI[i].(map[interface{}]interface{}) {
			boxes[i] = NewFilterBox(cfg.(map[interface{}]interface{}))
			boxes[i].Filter = filters[i]
		}
	}

	return boxes
}
