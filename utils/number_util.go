package utils

import (
    "errors"
)

var EmptyError = errors.New("value is nil")
var InValidType = errors.New("not valid type")

func ToInt64(value interface{}) (int64, error) {
    if value == nil {
        return -1, EmptyError
    }
    v1, validType := value.(int64)
    if validType {
        return v1, nil
    }

    v12, validType := value.(float64)
    if validType {
        return int64(v12), nil
    }

    v2, validType := value.(int)
    if validType {
        return int64(v2), nil
    }

    v3, validType := value.(int32)
    if validType {
        return int64(v3), nil
    }

    v4, validType := value.(int16)
    if validType {
        return int64(v4), nil
    }
    v5, validType := value.(int8)
    if validType {
        return int64(v5), nil
    }
    v6, validType := value.(uint64)
    if validType {
        return int64(v6), nil
    }
    v7, validType := value.(uint)
    if validType {
        return int64(v7), nil
    }
    v8, validType := value.(uint32)
    if validType {
        return int64(v8), nil
    }
    v9, validType := value.(uint16)
    if validType {
        return int64(v9), nil
    }
    v10, validType := value.(uint8)
    if validType {
        return int64(v10), nil
    }

    v11, validType := value.(float32)
    if validType {
        return int64(v11), nil
    }

    return -1, InValidType
}

func ToString(value interface{}) (string, error) {
    if value == nil {
        return "", nil
    }
    if strValue, validType := value.(string); validType {
        return strValue, nil
    }
    return "", InValidType
}

func ConvInterfaceToInt(value interface{}) int {
    if v, valid := value.(int); valid {
        return v
    }
    return 0
}