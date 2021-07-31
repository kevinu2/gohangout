package utils

import (
    "strings"
)

func StrIsEmpty(str string) bool {
    return strings.TrimSpace(str) == ""
}

func ConvInterfaceToStr(value interface{}) string {
    if v, valid := value.(string); valid {
        return v
    }
    return ""
}
