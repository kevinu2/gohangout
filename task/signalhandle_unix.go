// +build linux darwin

package task

import (
	"github.com/golang/glog"
	"github.com/kevinu2/gohangout/cfg"
	"os"
	"os/signal"
	"syscall"
)

func listenSignal(inputs GoHangoutInputs, configChannel chan map[string]interface{}, hangoutTask *HangoutTask) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)

rangeC:
	for sig := range c {
		glog.Infof("capture signal: %v", sig)
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			inputs.Stop()
			close(configChannel)
			break rangeC
		case syscall.SIGUSR1:
			// `kill -USR1 pid`也会触发重新加载
			config := hangoutTask.Config
			if config != nil {
				glog.Infof("config:\n%s", cfg.RemoveSensitiveInfo(config))
				configChannel <- config
			}
		}
	}
	glog.Infof("listen signal stops, exit...")
	hangoutTask.Exit()
}
