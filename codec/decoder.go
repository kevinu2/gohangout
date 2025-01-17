package codec

import (
	"plugin"

	"github.com/golang/glog"
)

const (
	UnParsedMessageField = "unparsed_message"
)

type Decoder interface {
	Decode([]byte) map[string]interface{}
}

func NewDecoder(t string) Decoder {
	switch t {
	case "plain":
		return &PlainDecoder{}
	case "json":
		return &JsonDecoder{useNumber: true}
	case "json:not_usenumber":
		return &JsonDecoder{useNumber: false}
	case "shm":
		return &ShmDecoder{}
	case "shm_json":
		return &ShmJsonDecoder{useNumber: true}
	default:
		p, err := plugin.Open(t)
		if err != nil {
			glog.Fatalf("could not open %s: %s", t, err)
		}
		newFunc, err := p.Lookup("New")
		if err != nil {
			glog.Fatalf("could not find New function in %s: %s", t, err)
		}
		return newFunc.(func() interface{})().(Decoder)
	}
}
