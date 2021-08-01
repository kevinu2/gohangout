package rpc

import (
    "github.com/golang/glog"
    "github.com/kevinu2/gohangout/cfg"
    "github.com/smallnest/rpcx/server"
)

var rpcServer *server.Server

func StartRpcServer() {
    config := cfg.GetAppConfig().RpcConfig
    rpcServer := server.NewServer()
    err := rpcServer.Register(new(EtlTask), "")
    if err != nil {
        glog.Fatal(err)
    }
    err = rpcServer.Serve("tcp", config.Address)
    if err != nil {
        glog.Fatal(err)
    }
}

func StopRpcServer()  {
    if rpcServer != nil {
        _ = rpcServer.Close()
    }
}