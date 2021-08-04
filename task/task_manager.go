package task

import (
	"encoding/base64"
	"errors"
	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/kevinu2/gohangout/cfg"
	"github.com/kevinu2/gohangout/common"
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
	TaskInstanceId string
	sync.Mutex
}

type TskRpcRequest struct {
	UploadContent string
}

var (
	taskManager  *TskManager
	once         sync.Once
)


func GetRunningKey(vendor string, ruleId string) string {
	return strings.Join([]string{vendor,ruleId}, ":")
}

func getRunningKey(task *HangoutTask) string {
	return GetRunningKey(task.Vendor, task.RuleId)
}

func deleteTaskCache(task *HangoutTask) {
	taskManager.Lock()
	defer taskManager.Unlock()
	delete(taskManager.taskCache, task.TaskId)
	task.stopTask()
}

func readOneTaskFromCache() *HangoutTask {
	taskManager.Lock()
	defer taskManager.Unlock()
	exists := len(taskManager.taskCache) > 0
	if !exists {
		glog.Infof("not found running task")
		return nil
	}
	var task *HangoutTask
	for _, v := range taskManager.taskCache {
		task = v
		break
	}
	return task
}

func taskIsRunning(task *HangoutTask) bool {
	if task == nil {
		return false
	}
	return task.TaskStatus == Starting
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

func GenerateStartParam(config *string, loadMode common.RuleLoadMode, startInstant bool) (*TskStartParam, error) {
	var ruleCfg map[string]interface{}
	var fileName string
	var base64Config *string
	if utils.StrIsEmpty(*config) {
		return nil, errors.New("rule content is empty")
	}
	if loadMode == common.Rpc {
		rpcCfg, err := parseBase64RuleCfg(config)
		if err != nil {
			return nil, err
		}
		ruleCfg = rpcCfg
		base64Config = config
	} else {
		fileCfg, err := cfg.ParseConfig(*config)
		if err != nil {
			glog.Errorf("could not parse config: %v", err)
			return nil, err
		}
		ruleCfg = fileCfg
		fileName = *config
	}
	param := &TskStartParam{
		config: ruleCfg,
		fileName: fileName,
		commitChan: make(chan TskActionResult),
		ruleLoadMode: loadMode,
		base64Config: base64Config,
		StartInstant: startInstant,
	}
	return param, nil
}

func handleCommitResult(commitResult *TskActionResult, task *HangoutTask)  {
	taskManager.Lock()
	defer taskManager.Unlock()
	for k := range taskManager.taskCache {
		delete(taskManager.taskCache, k)
	}
	if utils.StrIsEmpty(task.TaskId) {
		task.TaskId = uuid.New().String()
		commitResult.TaskId = task.TaskId
	}
	taskManager.taskCache[task.TaskId] = task
	runningKey := getRunningKey(task)
	glog.Infof("add task to cache with task-id =%s,running-key=%s", task.TaskId, runningKey)
}

func GenerateHangoutTask(param *TskStartParam, tskManager *TskManager) (*HangoutTask,error) {
	config := param.config
	fileName := param.fileName
	cmdOptions := tskManager.CmdOptions
	worker := cmdOptions.Worker
	if v, ok := config["worker"]; ok {
		configWorker := utils.ConvInterfaceToInt(v)
		if configWorker > 0 {
			worker = configWorker
		}
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
	task := &HangoutTask{
		Config: config,
		Worker: worker,
		ExitWhenNil: cmdOptions.ExitWhenNil,
		AutoReload: cmdOptions.AutoReload,
		FileName: fileName,
		MainThreadExitChan: tskManager.MainThreadExitChan,
		RuleId: ruleId,
		RuleName: ruleName,
		Vendor: vendor,
		Description: description,
		TaskStatus: Unupload,
	}
	task.CreateTime = time.Now().Format( "2006-01-02 15:04:05.000")
    if param.base64Config != nil {
    	task.Base64Config =  *param.base64Config
	}
	//err := checkRpcRequestArgs(task, param.ruleLoadMode)
	//if err != nil {
	//	return nil, err
	//}
	return task, nil
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

func (tskManager *TskManager) startHangoutTask(param *TskStartParam, hangoutTask *HangoutTask) *TskActionResult {
	commitResult := &TskActionResult{Success: false}
	commitResult.RuleId = hangoutTask.RuleId
	commitResult.Vendor = hangoutTask.Vendor
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

func LoadFromCmd() {
	cmdOptions := taskManager.CmdOptions
	param, err := GenerateStartParam(&cmdOptions.ConfigFilePath, common.Cmd, true)
	if err != nil {
		glog.Error(err)
		return
	}
	hangoutTask, _ := GenerateHangoutTask(param, taskManager)
	commitResult := taskManager.startHangoutTask(param, hangoutTask)
	handleCommitResult(commitResult, hangoutTask)
}

func LoadFromConfigDir() {
	ruleFiles := common.LoadRuleFromConfigDir()
	if len(ruleFiles) == 0 {
		glog.Warning("could not find valid rule file at the dir config/rules")
	}
	for _, file := range ruleFiles {
		param, err := GenerateStartParam(&file, common.ConfigDir, true)
		if err != nil {
			glog.Error(err)
			continue
		}
		hangoutTask, _ := GenerateHangoutTask(param, taskManager)
		commitResult := taskManager.startHangoutTask(param, hangoutTask)
		handleCommitResult(commitResult, hangoutTask)
	}
}

func GetTaskManager() *TskManager {
	once.Do(func() {
		taskManager = new(TskManager)
		taskManager.taskCache = make(map[string]*HangoutTask, 0)
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

func RegisterToEtcd() {
	rpcCfg := cfg.GetAppConfig().RpcConfig
	etcdAddr := rpcCfg.EtcdAddr
	if len(etcdAddr) == 0{
		glog.Fatal("etcd cluster address is empty")
	}
	port := rpcCfg.Port
	if port <= 0 {
		glog.Fatalf("rpc port=%d is invalid", port)
	}
	createEtcdClient(etcdAddr)
	uid := strings.ReplaceAll(uuid.New().String(), "-", "")
	registerRpcServiceToEtc(rpcCfg.RegistryPrefix, port, uid)
	configEtcdPrefix := rpcCfg.ConfigPrefix
	instancePrefix := configEtcdPrefix + ".instance"
	taskManager.TaskInstanceId = instancePrefix + "." + uid
	glog.Infof("current process instance-id=%s", taskManager.TaskInstanceId)
}

