package filter

import (
	"encoding/json"
	"errors"
	"reflect"
	"strconv"

	"github.com/golang/glog"
	"github.com/kevinu2/gohangout/field_setter"
	"github.com/kevinu2/gohangout/topology"
	"github.com/kevinu2/gohangout/value_render"
)

type Converter interface {
	convert(v interface{}) (interface{}, error)
}

var ErrConvertUnknownFormat = errors.New("unknown format")

type IntConverter struct{}

func (c *IntConverter) convert(v interface{}) (interface{}, error) {
	if reflect.TypeOf(v).String() == "json.Number" {
		return v.(json.Number).Int64()
	}

	if reflect.TypeOf(v).Kind() == reflect.String {
		return strconv.ParseInt(v.(string), 0, 64)
	}
	return nil, ErrConvertUnknownFormat
}

type FloatConverter struct{}

func (c *FloatConverter) convert(v interface{}) (interface{}, error) {
	if reflect.TypeOf(v).String() == "json.Number" {
		return v.(json.Number).Float64()
	}
	if reflect.TypeOf(v).Kind() == reflect.String {
		return strconv.ParseFloat(v.(string), 64)
	}
	return nil, ErrConvertUnknownFormat
}

type BoolConverter struct{}

func (c *BoolConverter) convert(v interface{}) (interface{}, error) {
	return strconv.ParseBool(v.(string))
}

type StringConverter struct{}

func (c *StringConverter) convert(v interface{}) (interface{}, error) {
	if r, ok := v.(json.Number); ok {
		return r.String(), nil
	}

	if r, ok := v.(string); ok {
		return r, nil
	}

	jsonString, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return string(jsonString), nil
}

type ArrayIntConverter struct{}

func (c *ArrayIntConverter) convert(v interface{}) (interface{}, error) {
	if v1, ok1 := v.([]interface{}); ok1 {
		var t2 []int
		for _, i := range v1 {
			j, err := i.(json.Number).Int64()
			// j, err := strconv.ParseInt(i.String(), 0, 64)
			if err != nil {
				return nil, ErrConvertUnknownFormat
			}
			t2 = append(t2, (int)(j))
		}
		return t2, nil
	}
	return nil, ErrConvertUnknownFormat
}

type ArrayFloatConverter struct{}

func (c *ArrayFloatConverter) convert(v interface{}) (interface{}, error) {
	if v1, ok1 := v.([]interface{}); ok1 {
		var t2 []float64
		for _, i := range v1 {
			j, err := i.(json.Number).Float64()
			if err != nil {
				return nil, ErrConvertUnknownFormat
			}
			t2 = append(t2, j)
		}
		return t2, nil
	}
	return nil, ErrConvertUnknownFormat
}

type ConverterAndRender struct {
	converter    Converter
	valueRender  value_render.ValueRender
	removeIfFail bool
	setToIfFail  interface{}
	setToIfNil   interface{}
}

type ConvertFilter struct {
	config map[interface{}]interface{}
	fields map[field_setter.FieldSetter]ConverterAndRender
}

func init() {
	Register("Convert", newConvertFilter)
}

func newConvertFilter(config map[interface{}]interface{}) topology.Filter {
	plugin := &ConvertFilter{
		config: config,
		fields: make(map[field_setter.FieldSetter]ConverterAndRender),
	}

	if fieldsValue, ok := config["fields"]; ok {
		for f, vI := range fieldsValue.(map[interface{}]interface{}) {
			v := vI.(map[interface{}]interface{})
			fieldSetter := field_setter.NewFieldSetter(f.(string))
			if fieldSetter == nil {
				glog.Fatalf("could build field setter from %s", f.(string))
			}

			to := v["to"].(string)
			removeIfFail := false
			if I, ok := v["remove_if_fail"]; ok {
				removeIfFail = I.(bool)
			}
			setToIfFail := v["setto_if_fail"]
			setToIfNil := v["setto_if_nil"]

			var converter Converter
			if to == "float" {
				converter = &FloatConverter{}
			} else if to == "int" {
				converter = &IntConverter{}
			} else if to == "bool" {
				converter = &BoolConverter{}
			} else if to == "string" {
				converter = &StringConverter{}
			} else if to == "array(int)" {
				converter = &ArrayIntConverter{}
			} else if to == "array(float)" {
				converter = &ArrayFloatConverter{}
			} else {
				glog.Fatal("can only convert to int/float/bool/array(int)/array(float)")
			}

			plugin.fields[fieldSetter] = ConverterAndRender{
				converter:    converter,
				valueRender:  value_render.GetValueRender2(f.(string)),
				removeIfFail: removeIfFail,
				setToIfFail:  setToIfFail,
				setToIfNil:   setToIfNil,
			}
		}
	} else {
		glog.Fatal("fields must be set in convert filter plugin")
	}
	return plugin
}

func (plugin *ConvertFilter) Filter(event map[string]interface{}) (map[string]interface{}, bool) {
	for fs, converterAndRender := range plugin.fields {
		originalV := converterAndRender.valueRender.Render(event)
		if originalV == nil {
			if converterAndRender.setToIfNil != nil {
				event = fs.SetField(event, converterAndRender.setToIfNil, "", true)
			}
			continue
		}
		v, err := converterAndRender.converter.convert(originalV)
		if err == nil {
			event = fs.SetField(event, v, "", true)
		} else {
			glog.V(10).Infof("convert error: %s", err)
			if converterAndRender.removeIfFail {
				event = fs.SetField(event, nil, "", true)
			} else if converterAndRender.setToIfFail != nil {
				event = fs.SetField(event, converterAndRender.setToIfFail, "", true)
			}
		}
	}
	return event, true
}
