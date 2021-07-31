package task

import (
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/kevinu2/gohangout/cfg"
	"github.com/kevinu2/gohangout/common"
	"github.com/kevinu2/gohangout/db"
	"github.com/kevinu2/gohangout/utils"
	"strings"
	"sync"
	"time"
)

type CmdOptions struct {
	AutoReload bool
	ExitWhenNil bool
	Worker     int
	TaskShard string
	ConfigFilePath string
}

type TskManager struct {
	MainThreadExitChan chan struct{}
	CmdOptions CmdOptions
	taskCache  map[string]*HangoutTask
	runningTaskCache map[string]*HangoutTask
	sync.Mutex
}

var (
	taskManager  *TskManager
	once         sync.Once
)

func addToTaskCache(task *HangoutTask) {
	taskManager.Lock()
	defer taskManager.Unlock()
	taskManager.taskCache[task.TaskId] = task
	ruleId := task.RuleId
	vendor := task.Vendor
	taskShard := task.TaskShard
	runningKey := strings.Join([]string{vendor,ruleId,taskShard}, ":")
	taskManager.runningTaskCache[runningKey] = task
	glog.Infof("add task to cache with running key = %s", runningKey)
}

func taskIsRunning(task *HangoutTask) bool {
	ruleId := task.RuleId
	vendor := task.Vendor
	taskShard := task.TaskShard
	runningKey := strings.Join([]string{vendor,ruleId,taskShard}, ":")
	runningCache := taskManager.runningTaskCache
	t, exist := runningCache[runningKey]
	if !exist {
		return false
	}
	task.TaskId = t.TaskId
	isRunning := true
	for _, box := range t.inputs {
		if box.IsStop() {
		    isRunning = false
		    break
		}
	}
	if !isRunning {
		t.inputs.Stop()
		delete(runningCache, runningKey)
		delete(taskManager.taskCache, t.TaskId)
	}
	return isRunning
}

func parseBase64RuleCfg(ruleContent *string) (map[string]interface{}, error) {
	bytes, err := base64.StdEncoding.DecodeString(*ruleContent)
	commitResult := TskCommitResult{Success: false}
	if err != nil {
		commitResult.Err = err
		return nil, err
	}
	return cfg.ParseBytesConfig(bytes)
}

func (tskManager *TskManager) UploadRuleTask(args *common.UploadArgs) *TskCommitResult {
	 commitResult :=  tskManager.StartHangoutTask(&args.Content,  common.Rpc)
	 handleCommitResult(commitResult, false)
	 return commitResult
}

func (tskManager *TskManager) StartHangoutTask(config *string, loadMode common.RuleLoadMode) *TskCommitResult {
    var ruleCfg map[string]interface{}
    var fileName string
    var base64Config *string
	commitResult := TskCommitResult{Success: false}
	if loadMode == common.Rpc {
	    rpcCfg, err := parseBase64RuleCfg(config)
	    if err != nil {
		    commitResult.Err = err
		    return &commitResult
	    }
	    ruleCfg = rpcCfg
	    base64Config = config
    } else {
	    fileCfg, err := cfg.ParseConfig(*config)
	    if err != nil {
		    glog.Errorf("could not parse config: %v", err)
		    commitResult.Err = err
		    return &commitResult
	    }
	    ruleCfg = fileCfg
	    fileName = *config
    }
	param := &StartTaskParam{
		config: ruleCfg,
		fileName: fileName,
		commitChan: make(chan TskCommitResult),
		ruleLoadMode: loadMode,
		base64Config: base64Config,
	}
	return taskManager.startHangoutTask(param)
}

func saveToDb(param *StartTaskParam, hangoutTask *HangoutTask) error {
	ruleTask := new(RuleTask)
	ruleTask.Id = hangoutTask.TaskId
	ruleTask.RuleContent = *param.base64Config
	ruleTask.RuleId = hangoutTask.RuleId
	ruleTask.RuleName = hangoutTask.RuleName
	ruleTask.Shard = hangoutTask.TaskShard
	ruleTask.Vendor = hangoutTask.Vendor
	return taskDbService.saveTask(ruleTask)
}

func handleRpcCommit(commitResult *TskCommitResult, param *StartTaskParam, hangoutTask *HangoutTask) {
	err := saveToDb(param, hangoutTask)
	if err == nil {
		addToTaskCache(hangoutTask)
		return
	}
	commitResult.Success = false
	commitResult.Err = err
	hangoutTask.stopTask()
	return
}

func handleCommitResult(commitResult *TskCommitResult, isInitLoad bool)  {
	param := commitResult.Param
	hangoutTask := commitResult.Task
	if !commitResult.Success {
		hangoutTask.stopTask()
		return
	}
	if utils.StrIsEmpty(hangoutTask.TaskId) {
		hangoutTask.TaskId = uuid.New().String()
		commitResult.TaskId = hangoutTask.TaskId
	}
	if param.ruleLoadMode == common.Rpc && !isInitLoad {
		handleRpcCommit(commitResult, param, hangoutTask)
		return
	}
	addToTaskCache(hangoutTask)
}

func (tskManager *TskManager) parseStartTaskParam(param *StartTaskParam) *HangoutTask {
	config := param.config
	fileName := param.fileName
	cmdOptions := tskManager.CmdOptions
	worker := cmdOptions.Worker
	if v, ok := config["worker"]; ok {
		worker = v.(int)
	}
	if worker <= 0 {
		worker = 1
	}
	ruleId := ""
	if v, ok := config["id"]; ok {
		ruleId = v.(string)
	}
	ruleName := ""
	if v, ok := config["name"]; ok {
		ruleName = v.(string)
	}
    vendor := ""
	if v, ok := config["vendor"]; ok {
		vendor = v.(string)
	}
	if utils.StrIsEmpty(vendor) {
		vendor = common.DefaultTaskVendor
	}
	description := ""
	if v, ok := config["description"]; ok {
		description = v.(string)
	}
	return &HangoutTask{
		Config: config,
		Worker: worker,
		ExitWhenNil: cmdOptions.ExitWhenNil,
		AutoReload: cmdOptions.AutoReload,
		FileName: fileName,
		MainThreadExitChan: tskManager.MainThreadExitChan,
		RuleId: ruleId,
		RuleName: ruleName,
		TaskShard: cmdOptions.TaskShard,
		Vendor: vendor,
		Description: description,
	}
}

func (tskManager *TskManager) startHangoutTask(param *StartTaskParam) *TskCommitResult {
	commitResult := &TskCommitResult{Success: false}
	hangoutTask := tskManager.parseStartTaskParam(param)
	commitResult.RuleId = hangoutTask.RuleId
	commitResult.TaskShard = hangoutTask.TaskShard
	commitResult.Vendor = hangoutTask.Vendor
	commitResult.Param = param
	commitResult.Task = hangoutTask
	if taskIsRunning(hangoutTask) {
		commitResult.Err = errors.New(fmt.Sprintf("task "))
		commitResult.TaskId = hangoutTask.TaskId
		commitResult.Success = true
		glog.Infof("task with task id=%s has been running", hangoutTask.TaskId)
		return commitResult
	}
	go hangoutTask.startTask(param)
	select {
	case result := <- param.commitChan:
		commitResult.Success = result.Success
		commitResult.Err = result.Err
	case <-time.After(time.Second * 10):
		commitResult.Err = errors.New("start task timeout")
	}
	return commitResult
}

func loadFromDb () {
	shard := taskManager.CmdOptions.TaskShard
	if !utils.StrIsEmpty(shard) {
		err, tasks := taskDbService.queryTaskByShard(shard)
		if err != nil {
			glog.Error(err)
		}
		if len(tasks) == 0 {
		    return
		}
        for _, task := range tasks {
        	ruleContent := task.RuleContent
	        commitResult := taskManager.StartHangoutTask(&ruleContent, common.Rpc)
	        hangoutTask := commitResult.Task
	        hangoutTask.TaskId = task.Id
	        hangoutTask.RuleId = task.RuleId
	        hangoutTask.TaskShard = task.Shard
	        hangoutTask.Vendor = task.Vendor
	        handleCommitResult(commitResult, true)
        }
	}
}

func LoadFromCmd() {
	cmdOptions := taskManager.CmdOptions
	commitResult := taskManager.StartHangoutTask(&cmdOptions.ConfigFilePath, common.Cmd)
	handleCommitResult(commitResult, true)
}

func LoadFromConfigDir() {
	ruleFiles := common.LoadRuleFromConfigDir()
	if len(ruleFiles) == 0 {
		glog.Warning("could not find valid rule file at the dir config/rules")
	}
	for _, file := range ruleFiles {
		commitResult := taskManager.StartHangoutTask(&file, common.ConfigDir)
		handleCommitResult(commitResult, true)
	}
}

func LoadFromDb()  {
	db.InitDb()
	CreateRuleTaskTable()
	loadFromDb()
}

func GetTaskManager() *TskManager {
	once.Do(func() {
		taskManager = new(TskManager)
		taskManager.taskCache = make(map[string]*HangoutTask, 0)
		taskManager.runningTaskCache = make(map[string]*HangoutTask, 0)
	})
	return taskManager
}

func (tskManager *TskManager) StopAllTask()  {
	for _, task := range tskManager.taskCache {
		if task != nil {
			task.stopTask()
		}
	}
}