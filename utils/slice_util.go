package utils

func DistinctAddSliceString(slice *[]string, value string) {
    needAdd := true
    for _, v := range *slice {
        if v == value {
            needAdd = false
            break
        }
    }
    if needAdd {
        *slice = append(*slice, value)
    }
}
