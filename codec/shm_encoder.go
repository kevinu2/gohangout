package codec

import "errors"

type ShmEncoder struct{}

func (shm *ShmEncoder) Encode(v interface{}) ([]byte, error) {
	value, ok := v.(map[string]interface{})["content"]
	if ok{
		return []byte(value.(string)), nil
	}else{
		return nil, errors.New("there is no content in message")
	}
	return json.Marshal(v)
}