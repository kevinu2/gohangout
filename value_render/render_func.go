package value_render

import (
	"math/big"
	"net"
)

func ipV4toN(ip string) int64  {
	ipAddr := net.ParseIP(ip)
	if ipAddr == nil {
		return 0
	}
	ret := big.NewInt(0)
	ret.SetBytes(ipAddr.To4())
	return ret.Int64()
}
