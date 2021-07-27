package codec

import (
	"github.com/kevinu2/gohangout/simplejson"
)

type Encoder interface {
	Encode(interface{}) ([]byte, error)
}

func NewEncoder(t string) Encoder {
	switch t {
	case "json":
		return &JsonEncoder{}
	case "simplejson":
		return &simplejson.Decoder{}
	}
	panic(t + " encoder not supported")
	return nil
}
