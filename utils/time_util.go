package utils

import "time"

func UnixMillisToTime(millis int64) time.Time {
    seconds := millis / 1e3
    remain := millis % 1e3
    return time.Unix(seconds, remain*1e6)
}

func TimeToUnixMillis(timeValue time.Time) int64 {
    return timeValue.UnixNano() / 1e6
}
