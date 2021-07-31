// +build linux darwin

package task

import (
	"github.com/golang/glog"
	"os"
	"os/signal"
	"syscall"
)

func listenSignal(inputs GoHangoutInputs, configChannel chan map[string]interface{}) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)

rangeC:
	for sig := range c {
		glog.Infof("capture signal: %v", sig)
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			inputs.stop()
			close(configChannel)
			break rangeC
		case syscall.SIGUSR1:
			// `kill -USR1 pid`也会触发重新加载
			config, err := main.parseConfig(options.config)
			if err != nil {
				glog.Errorf("could not parse config:%s", err)
				continue
			}
			glog.Infof("config:\n%s", main.removeSensitiveInfo(config))
			configChannel <- config
		}
	}

	glog.Infof("listen signal stops, exit...")
	exit()
}
