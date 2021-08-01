package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/kevinu2/gohangout/cfg"
	"github.com/kevinu2/gohangout/common"
	"github.com/kevinu2/gohangout/rpc"
	"github.com/kevinu2/gohangout/task"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
)

var cmdOptions = &struct {
	config     string
	autoReload bool // 配置文件更新自动重启
	pprof      bool
	pprofAddr  string
	cpuProfile string
	memProfile string
	exitWhenNil bool
	minLogLevel int
	worker     int
	taskShard  string
}{}

var (
	mainThreadExitChan = make(chan struct{}, 0)
	ruleLoadMode  common.RuleLoadMode
	appConfig    *cfg.AppConfig
	taskManager *task.TskManager
)

func init() {
	flag.StringVar(&cmdOptions.config, "config", cmdOptions.config, "path to configuration file or directory")
	flag.BoolVar(&cmdOptions.autoReload, "reload", cmdOptions.autoReload, "if auto reload while config file changed")
	flag.BoolVar(&cmdOptions.pprof, "pprof", false, "pprof or not")
	flag.StringVar(&cmdOptions.pprofAddr, "pprof-address", "127.0.0.1:8899", "default: 127.0.0.1:8899")
	flag.StringVar(&cmdOptions.cpuProfile, "cpu-profile", "", "write cpu profile to `file`")
	flag.StringVar(&cmdOptions.memProfile, "mem-profile", "", "write mem profile to `file`")
	flag.BoolVar(&cmdOptions.exitWhenNil, "exit-when-nil", false, "triger gohangout to exit when receive a nil event")
	flag.IntVar(&cmdOptions.worker, "worker", 1, "worker thread count")
	flag.StringVar(&cmdOptions.taskShard, "task-shard", "0", "task running shard index")

	flag.Parse()
	cfg.InitAppConfig()
	appConfig = cfg.GetAppConfig()
	taskShardPrefix := ""
	if appConfig != nil {
		ruleLoadMode = appConfig.RuleLoadMode
		taskShardPrefix = appConfig.TaskShardPrefix
	}
	if ruleLoadMode == "" {
		ruleLoadMode = common.Cmd
	}
	cmdOptions := task.CmdOptions{
		AutoReload: cmdOptions.autoReload,
		ExitWhenNil: cmdOptions.exitWhenNil,
		Worker: cmdOptions.worker,
		ConfigFilePath: cmdOptions.config,
		TaskShard: strings.Join([]string{taskShardPrefix, cmdOptions.taskShard}, ""),
	}
	taskManager = task.GetTaskManager()
	taskManager.CmdOptions = cmdOptions
	taskManager.MainThreadExitChan = mainThreadExitChan
}


func main() {
	printVersion()
	defer glog.Flush()
	if cmdOptions.pprof {
		go func() {
			err := http.ListenAndServe(cmdOptions.pprofAddr, nil)
			if err != nil {
				glog.Error(err)
			}
		}()
	}
	if cmdOptions.cpuProfile != "" {
		f, err := os.Create(cmdOptions.cpuProfile)
		if err != nil {
			glog.Fatalf("could not create CPU profile: %s", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			glog.Fatalf("could not start CPU profile: %s", err)
		}
		defer pprof.StopCPUProfile()
	}

	if cmdOptions.memProfile != "" {
		defer func() {
			f, err := os.Create(cmdOptions.memProfile)
			if err != nil {
				glog.Fatalf("could not create memory profile: %s", err)
			}
			defer f.Close()
			runtime.GC() // get up-to-date statistics
			if err := pprof.WriteHeapProfile(f); err != nil {
				glog.Fatalf("could not write memory profile: %s", err)
			}
		}()
	}
	if ruleLoadMode == common.Cmd {
		task.LoadFromCmd()
	} else if ruleLoadMode == common.ConfigDir {
		task.LoadFromConfigDir()
	} else if ruleLoadMode == common.Rpc {
		task.LoadFromDb()
		rpc.StartRpcServer()
	}
	<-mainThreadExitChan
	taskManager.StopAllTask()
	rpc.StopRpcServer()
}

