package field_deleter

import "regexp"

type FieldDeleter interface {
	Delete(map[string]interface{})
}

func NewFieldDeleter(template string) FieldDeleter {
	matches, _ := regexp.Compile(`(\[.*?\])+`)
	finds, _ := regexp.Compile(`(\[(.*?)\])`)
	if matches.Match([]byte(template)) {
		fields := make([]string, 0)
		for _, v := range finds.FindAllStringSubmatch(template, -1) {
			fields = append(fields, v[2])
		}
		return NewMultiLevelFieldDeleter(fields)
	} else {
		return NewOneLevelFieldDeleter(template)
	}
}
