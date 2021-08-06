package codec

import (
	"bytes"
	"time"
	"unsafe"
)

type ShmJsonDecoder struct {
	useNumber bool
}

func (shm *ShmJsonDecoder) Decode(value []byte) map[string]interface{} {
	rst := make(map[string]interface{})
	var tlvHead TagTLVHead = **(**TagTLVHead)(unsafe.Pointer(&value))
	//var tlvHead *TagTLVHead = *(**TagTLVHead)(unsafe.Pointer(&value))
	headSize := unsafe.Sizeof(tlvHead)

	rst["@timestamp"] = time.Now()
	d := json.NewDecoder(bytes.NewReader(value[headSize:tlvHead.Len]))

	if shm.useNumber {
		d.UseNumber()
	}
	err := d.Decode(&rst)
	if err != nil || d.More() {
		return map[string]interface{}{
			"@timestamp": time.Now(),
			"message":    string(value),
		}
	}
	return rst
}
