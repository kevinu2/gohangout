package field_setter

import "regexp"

type FieldSetter interface {
	SetField(map[string]interface{}, interface{}, string, bool) map[string]interface{}
}

var (
	matchPattern, _ = regexp.Compile(`(\[.*?\])+`)
	findPattern, _ = regexp.Compile(`(\[(.*?)\])`)
)

func NewFieldSetter(template string) FieldSetter {
	if matchPattern.Match([]byte(template)) {
		fields := make([]string, 0)
		for _, v := range findPattern.FindAllStringSubmatch(template, -1) {
			fields = append(fields, v[2])
		}
		if len(fields) == 1 {
			return NewOneLevelFieldSetter(fields[0])
		}
		return NewMultiLevelFieldSetter(fields)
	} else {
		return NewOneLevelFieldSetter(template)
	}
}
