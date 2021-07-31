// +build windows

package task

import (
	"github.com/golang/glog"
	"os"
	"os/signal"
	"syscall"
)

func listenSignal(inputs GoHangoutInputs, configChannel chan map[string]interface{}) {
	c := make(chan os.Signal, 1)
	var stop bool
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	defer glog.Infof("listen signal stop, exit...")

	for sig := range c {
		glog.Infof("capture signal: %v", sig)
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			inputs.Stop()
			close(configChannel)
			stop = true
		}
		if stop {
			break
		}
	}
}
