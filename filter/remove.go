package filter

import (
	"github.com/golang/glog"
	"github.com/kevinu2/gohangout/field_deleter"
	"github.com/kevinu2/gohangout/topology"
)

type RemoveFilter struct {
	config             map[interface{}]interface{}
	fieldsDeleterSlice []field_deleter.FieldDeleter
}

func init() {
	Register("Remove", newRemoveFilter)
}

func newRemoveFilter(config map[interface{}]interface{}) topology.Filter {
	plugin := &RemoveFilter{
		config:             config,
		fieldsDeleterSlice: make([]field_deleter.FieldDeleter, 0),
	}

	if fieldsValue, ok := config["fields"]; ok {
		for _, field := range fieldsValue.([]interface{}) {
			plugin.fieldsDeleterSlice = append(plugin.fieldsDeleterSlice, field_deleter.NewFieldDeleter(field.(string)))
		}
	} else {
		glog.Fatal("fields must be set in remove filter plugin")
	}
	return plugin
}

func (plugin *RemoveFilter) Filter(event map[string]interface{}) (map[string]interface{}, bool) {
	for _, d := range plugin.fieldsDeleterSlice {
		d.Delete(event)
	}
	return event, true
}
