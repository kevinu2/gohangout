package codec

import (
	"unsafe"
)

type ShmDecoder struct {
	useNumber bool
}

type TagTLVHead struct {
	Tag          int64
	Len          uint64
	TopicLen     uint16
	EventTypeLen uint16
	Topic        [30]byte
	EventType    [30]byte
}

func (shm *ShmDecoder) Decode(value []byte) map[string]interface{} {
	rst := make(map[string]interface{})
	var tlvHead TagTLVHead = **(**TagTLVHead)(unsafe.Pointer(&value))
	//var tlvHead *TagTLVHead = *(**TagTLVHead)(unsafe.Pointer(&value))
	rst["event_type"] = string(tlvHead.EventType[:tlvHead.EventTypeLen])
	rst["topic"] = string(tlvHead.Topic[:tlvHead.TopicLen])
	headSize := unsafe.Sizeof(tlvHead)
	//todo test content with gb2312 code data
	rst["content"] = string(value[headSize:tlvHead.Len])
	return rst
}