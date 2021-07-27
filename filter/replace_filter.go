package filter

import (
	"strings"

	"github.com/golang/glog"
	"github.com/kevinu2/gohangout/field_setter"
	"github.com/kevinu2/gohangout/topology"
	"github.com/kevinu2/gohangout/value_render"
)

type replaceConfig struct {
	s   field_setter.FieldSetter
	v   value_render.ValueRender
	old string
	new string
	n   int
}

type ReplaceFilter struct {
	config map[interface{}]interface{}
	fields []replaceConfig
}

func init() {
	Register("Replace", newReplaceFilter)
}

func newReplaceFilter(config map[interface{}]interface{}) topology.Filter {
	p := &ReplaceFilter{
		config: config,
		fields: make([]replaceConfig, 0),
	}

	if fieldsI, ok := config["fields"]; ok {
		for fieldI, configI := range fieldsI.(map[interface{}]interface{}) {
			fieldSetter := field_setter.NewFieldSetter(fieldI.(string))
			if fieldSetter == nil {
				glog.Fatalf("could build field setter from %s", fieldI.(string))
			}

			v := value_render.GetValueRender2(fieldI.(string))

			rConfig := configI.([]interface{})
			if len(rConfig) == 2 {
				t := replaceConfig{
					fieldSetter,
					v,
					rConfig[0].(string),
					rConfig[1].(string),
					-1,
				}
				p.fields = append(p.fields, t)
			} else if len(rConfig) == 3 {
				t := replaceConfig{
					fieldSetter,
					v,
					rConfig[0].(string),
					rConfig[1].(string),
					rConfig[2].(int),
				}
				p.fields = append(p.fields, t)
			} else {
				glog.Fatal("invalid fields config in replace filter")
			}
		}
	} else {
		glog.Fatal("fields must be set in replace filter plugin")
	}
	return p
}

// Filter 如果字段不是字符串, 返回false, 其它返回true
func (p *ReplaceFilter) Filter(event map[string]interface{}) (map[string]interface{}, bool) {
	success := true
	for _, f := range p.fields {
		value := f.v.Render(event)
		if value == nil {
			success = false
			continue
		}
		if s, ok := value.(string); ok {
			replace := strings.Replace(s, f.old, f.new, f.n)
			f.s.SetField(event, replace, "", true)
		} else {
			success = false
		}
	}
	return event, success
}
