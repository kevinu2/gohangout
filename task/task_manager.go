package task

import (
	"encoding/base64"
	"errors"
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

type TskRpcRequest struct {
	UploadContent string
    TaskId string
}

var (
	taskManager  *TskManager
	once         sync.Once
)


func getRunningKey(task *HangoutTask) string {
	ruleId := task.RuleId
	vendor := task.Vendor
	taskShard := task.TaskShard
	return strings.Join([]string{vendor,ruleId,taskShard}, ":")
}

func deleteTaskCache(runningKey string, task *HangoutTask) {
	taskManager.Lock()
	defer taskManager.Unlock()
	if runningKey == "" {
		runningKey = getRunningKey(task)
	}
	runningCache := taskManager.runningTaskCache
	delete(runningCache, runningKey)
	delete(taskManager.taskCache, task.TaskId)
	task.stopTask()
}

func statusName(status RunningStatus) string {
	if status == Running {
		return "running"
	}
	return "Stopped"
}

func addToTaskCache(task *HangoutTask) {
	taskManager.Lock()
	defer taskManager.Unlock()
	taskManager.taskCache[task.TaskId] = task
	runningKey := getRunningKey(task)
	taskManager.runningTaskCache[runningKey] = task
	glog.Infof("add task to cache with task-id =%s,running-key=%s,status=%s", task.TaskId, runningKey, statusName(task.TaskStatus))
}

func readTaskFrmCache(taskId string) *HangoutTask {
	taskManager.Lock()
	defer taskManager.Unlock()
	task, exists := taskManager.taskCache[taskId]
	if !exists {
		glog.Infof("task with id = %s not exists", taskId)
		return nil
	}
	return task
}

func taskIsRunning(task *HangoutTask) bool {
	runningKey := getRunningKey(task)
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
		deleteTaskCache(runningKey, task)
	}
	return isRunning
}

func parseBase64RuleCfg(ruleContent *string) (map[string]interface{}, error) {
	bytes, err := base64.StdEncoding.DecodeString(*ruleContent)
	commitResult := TskActionResult{Success: false}
	if err != nil {
		commitResult.Err = err
		return nil, err
	}
	return cfg.ParseBytesConfig(bytes)
}

func (tskManager *TskManager) UploadRpcTask(args *TskRpcRequest) *TskActionResult {
	 commitResult :=  tskManager.StartHangoutTask(&args.UploadContent, common.Rpc, true)
	 handleCommitResult(commitResult, false)
	 return commitResult
}

func (tskManager *TskManager) StartOrKillRpcTask(args *TskRpcRequest, kill bool) *TskActionResult {
	taskId := args.TaskId
	actionResult := &TskActionResult{
		Success: true,
		TaskId: taskId,
	}
	updatedStatus := Stopped
	if !kill {
		updatedStatus = Running
	}
	task := readTaskFrmCache(taskId)
	if task == nil {
		return actionResult
	}
	//更新数据库失败
	err := taskDbService.updateTaskStatus(taskId, int16(updatedStatus))
	if err != nil {
		actionResult.Err = err
		actionResult.Success = false
		return actionResult
	}
	task.TaskStatus = updatedStatus
	actionResult.RuleId = task.RuleId
	actionResult.Vendor = task.Vendor
	actionResult.TaskShard = task.TaskShard
	if kill {
		task.stopTask()
	} else {
		commitResult := tskManager.restartRpcTask(task)
		handleCommitResult(commitResult, false)
	}
	return actionResult
}

func (tskManager *TskManager) DeleteRpcTask(args *TskRpcRequest) *TskActionResult {
	taskId := args.TaskId
	actionResult := &TskActionResult{
		Success: true,
		TaskId: taskId,
	}
	task := readTaskFrmCache(taskId)
	if task == nil {
		return actionResult
	}
	err := taskDbService.deleteTaskById(taskId)
	if err != nil {
		actionResult.Err = err
		actionResult.Success = false
		return actionResult
	}
	actionResult.RuleId = task.RuleId
	actionResult.Vendor = task.Vendor
	actionResult.TaskShard = task.TaskShard
	deleteTaskCache("", task)
	return actionResult
}


func (tskManager *TskManager) restartRpcTask(task *HangoutTask) *TskActionResult {
	param := &StartTaskParam{
		config: task.Config,
		commitChan: make(chan TskActionResult),
		ruleLoadMode: common.Rpc,
		StartInstant: false,
		base64Config: &task.Base64Config,
	}
	return taskManager.startHangoutTask(param)
}

func (tskManager *TskManager) StartHangoutTask(config *string, loadMode common.RuleLoadMode, startInstant bool) *TskActionResult {
    var ruleCfg map[string]interface{}
    var fileName string
    var base64Config *string
	commitResult := &TskActionResult{Success: false}
	if utils.StrIsEmpty(*config) {
		commitResult.Err = errors.New("rule content is empty")
		return commitResult
	}
	if loadMode == common.Rpc {
	    rpcCfg, err := parseBase64RuleCfg(config)
	    if err != nil {
		    commitResult.Err = err
		    return commitResult
	    }
	    ruleCfg = rpcCfg
	    base64Config = config
    } else {
	    fileCfg, err := cfg.ParseConfig(*config)
	    if err != nil {
		    glog.Errorf("could not parse config: %v", err)
		    commitResult.Err = err
		    return commitResult
	    }
	    ruleCfg = fileCfg
	    fileName = *config
    }
	param := &StartTaskParam{
		config: ruleCfg,
		fileName: fileName,
		commitChan: make(chan TskActionResult),
		ruleLoadMode: loadMode,
		base64Config: base64Config,
		StartInstant: startInstant,
	}
	return taskManager.startHangoutTask(param)
}

func saveOrUpdateToDb(param *StartTaskParam, hangoutTask *HangoutTask) error {
	ruleTask := new(RuleTask)
	ruleTask.Id = hangoutTask.TaskId
	ruleTask.RuleContent = *param.base64Config
	ruleTask.RuleId = hangoutTask.RuleId
	ruleTask.RuleName = hangoutTask.RuleName
	ruleTask.Shard = hangoutTask.TaskShard
	ruleTask.Vendor = hangoutTask.Vendor
	ruleTask.Status = int16(hangoutTask.TaskStatus)
	ruleTask.Description = hangoutTask.Description
	return taskDbService.saveOrUpdateTask(ruleTask)
}

func handleRpcCommit(commitResult *TskActionResult, param *StartTaskParam, hangoutTask *HangoutTask) {
	err := saveOrUpdateToDb(param, hangoutTask)
	if err == nil {
		addToTaskCache(hangoutTask)
		return
	}
	commitResult.Success = false
	commitResult.Err = err
	hangoutTask.stopTask()
	hangoutTask.TaskStatus = Stopped
}

func handleCommitResult(commitResult *TskActionResult, isInitLoad bool)  {
	param := commitResult.Param
	hangoutTask := commitResult.Task
	if param == nil || hangoutTask == nil {
		return
	}
	if !commitResult.Success {
		hangoutTask.stopTask()
		hangoutTask.TaskStatus = Stopped
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
		worker = utils.ConvInterfaceToInt(v)
	}
	if worker <= 0 {
		worker = 1
	}
	ruleId := ""
	if v, ok := config["id"]; ok {
		ruleId = utils.ConvInterfaceToStr(v)
	}
	ruleName := ""
	if v, ok := config["name"]; ok {
		ruleName = utils.ConvInterfaceToStr(v)
	}
    vendor := ""
	if v, ok := config["vendor"]; ok {
		vendor = utils.ConvInterfaceToStr(v)
	}
	if utils.StrIsEmpty(vendor) {
		vendor = common.DefaultTaskVendor
	}
	description := ""
	if v, ok := config["description"]; ok {
		description = utils.ConvInterfaceToStr(v)
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
		TaskStatus: Stopped,
		Base64Config: *param.base64Config,
	}
}

func checkRpcRequestArgs(task *HangoutTask, ruleLoadMode common.RuleLoadMode) error {
	if ruleLoadMode != common.Rpc {
		return nil
	}
	if utils.StrIsEmpty(task.RuleId) {
	 	return errors.New("rule field 'id' can't be empty")
	}
	if utils.StrIsEmpty(task.RuleName) {
		return errors.New("rule field 'name' can't be empty")
	}
	if utils.StrIsEmpty(task.Vendor) {
		return errors.New("rule field 'vendor' can't be empty")
	}
	return nil
}

func (tskManager *TskManager) startHangoutTask(param *StartTaskParam) *TskActionResult {
	commitResult := &TskActionResult{Success: false}
	hangoutTask := tskManager.parseStartTaskParam(param)
	commitResult.RuleId = hangoutTask.RuleId
	commitResult.TaskShard = hangoutTask.TaskShard
	commitResult.Vendor = hangoutTask.Vendor
	err := checkRpcRequestArgs(hangoutTask, param.ruleLoadMode)
	if err != nil {
		commitResult.Err = err
		return commitResult
	}
	commitResult.Param = param
	commitResult.Task = hangoutTask
	if taskIsRunning(hangoutTask) {
		commitResult.TaskId = hangoutTask.TaskId
		oldTask := readTaskFrmCache(hangoutTask.TaskId)
		if oldTask != nil {
			oldTask.inputs.Stop()
		}
		glog.Infof("task with task id=%s has been running, will rebuild it", hangoutTask.TaskId)
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
        	startInstant := true
        	if task.Status == int16(Stopped) {
				startInstant = false
			}
			commitResult := taskManager.StartHangoutTask(&ruleContent, common.Rpc, startInstant)
			hangoutTask := commitResult.Task
	        hangoutTask.TaskId = task.Id
	        hangoutTask.RuleId = task.RuleId
	        hangoutTask.TaskShard = task.Shard
	        hangoutTask.Vendor = task.Vendor
	        hangoutTask.TaskStatus = RunningStatus(task.Status)
	        handleCommitResult(commitResult, true)
        }
	}
}

func LoadFromCmd() {
	cmdOptions := taskManager.CmdOptions
	commitResult := taskManager.StartHangoutTask(&cmdOptions.ConfigFilePath, common.Cmd, true)
	handleCommitResult(commitResult, true)
}

func LoadFromConfigDir() {
	ruleFiles := common.LoadRuleFromConfigDir()
	if len(ruleFiles) == 0 {
		glog.Warning("could not find valid rule file at the dir config/rules")
	}
	for _, file := range ruleFiles {
		commitResult := taskManager.StartHangoutTask(&file, common.ConfigDir, true)
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