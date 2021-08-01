package rpc

import (
    "github.com/golang/glog"
    "github.com/kevinu2/gohangout/cfg"
    "github.com/smallnest/rpcx/server"
)

func StartRpcServer() {
    config := cfg.GetAppConfig().RpcConfig
    s := server.NewServer()
    err := s.Register(new(EtlTask), "")
    if err != nil {
        glog.Fatal(err)
    }
    err = s.Serve("tcp", config.Address)
    if err != nil {
        glog.Fatal(err)
    }
}