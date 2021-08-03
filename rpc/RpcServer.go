package rpc

import (
    "github.com/golang/glog"
    "github.com/kevinu2/gohangout/cfg"
    _ "github.com/rpcxio/rpcx-etcd/store/etcdv3"
    "github.com/smallnest/rpcx/server"
    "strconv"
)

var rpcServer *server.Server

func StartRpcServer() {
    rpcServer := server.NewServer()
    err := rpcServer.Register(new(HangoutTask), "")
    if err != nil {
        glog.Fatal(err)
    }
    config := cfg.GetAppConfig().RpcConfig
    port := config.Port
    if port <= 0 {
        glog.Fatalf("rpc port=%d is invalid", port)
    }
    err = rpcServer.Serve("tcp", ":" + strconv.Itoa(port))
    if err != nil {
       glog.Fatal(err)
    }
}

func StopRpcServer()  {
    if rpcServer != nil {
        _ = rpcServer.Close()
    }
}

