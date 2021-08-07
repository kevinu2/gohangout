package codec

import (
	"bytes"
	"github.com/golang/glog"
	"time"
)

type JsonDecoder struct {
	useNumber bool
}

func (jd *JsonDecoder) Decode(value []byte) map[string]interface{} {
	rst := make(map[string]interface{})
	rst["@timestamp"] = time.Now()
	d := json.NewDecoder(bytes.NewReader(value))

	if jd.useNumber {
		d.UseNumber()
	}
	err := d.Decode(&rst)
	if err != nil || d.More() {
		glog.V(10).Info(err)
		return map[string]interface{}{
			"@timestamp": time.Now(),
			 UnParsedMessageField:    string(value),
		}
	}

	return rst
}
