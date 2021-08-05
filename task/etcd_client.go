package task

import (
	"errors"
	"github.com/golang/glog"
	"github.com/kevinu2/gohangout/etcd"
	"net"
	"strconv"
	"time"
)

var serviceDiscoveryClient etcd.Store

func init() {
	etcd.Register()
}

func createEtcdClient(addr []string)   {
	kv, err := etcd.NewStore(etcd.HangoutEtcd3Backend, addr, nil)
	if err != nil {
		glog.Fatal(err)
	}
	serviceDiscoveryClient = kv
}

func getIpFromAddr(addr net.Addr) net.IP {
	var ip net.IP
	switch v := addr.(type) {
	case *net.IPNet:
		ip = v.IP
	case *net.IPAddr:
		ip = v.IP
	}
	if ip == nil || ip.IsLoopback() {
		return nil
	}
	ip = ip.To4()
	if ip == nil {
		return nil
	}
	return ip
}

func externalIP() (net.IP, error) {
	iFaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iFace := range iFaces {
		if iFace.Flags & net.FlagUp == 0 {
			continue // interface down
		}
		if iFace.Flags & net.FlagLoopback != 0 {
			continue // loopback interface
		}
		adds, err := iFace.Addrs()
		if err != nil {
			return nil, err
		}
		for _, addr := range adds {
			ip := getIpFromAddr(addr)
			if ip == nil {
				continue
			}
			return ip, nil
		}
	}
	return nil, errors.New("connected to the network fail")
}

func registerRpcServiceToEtc(registerPrefix string, port int, uuid string)  {
	ip, err := externalIP()
	if err != nil {
		glog.Fatal(err)
	}
	key := registerPrefix + "." + uuid
	value := "http://" + ip.String() + ":" + strconv.Itoa(port)
	writeOption := &etcd.WriteOptions{
		TTL: time.Second * 30,
	}
	err = serviceDiscoveryClient.Put(key, []byte(value), writeOption)
    if err != nil {
    	glog.Error("register to etcd fail:", err)
	} else {
		glog.Infof("register to etcd success %s=%s", key,value)
	}
}

func CloseEtcdClient()  {
	if serviceDiscoveryClient != nil {
		serviceDiscoveryClient.Close()
	}
}



