package task

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/kevinu2/gohangout/common"
	"github.com/kevinu2/gohangout/input"
	"github.com/kevinu2/gohangout/topology"
	_ "net/http/pprof"
	"sync"
)

type GoHangoutInputs []*input.InputBox

type TskActionResult struct {
	LoadMode common.RuleLoadMode
	TaskId   string
	Success bool
	Err     error
	Vendor string
	RuleId string
	RuleName string
	RuleDesc string
	TaskStatus RunningStatus
	Base64Config string
}

type RunningStatus string
const (
	Uploaded  RunningStatus = "uploaded"
	Unupload  RunningStatus = "noupload"
	Starting  RunningStatus = "starting"
	Stopped  RunningStatus ="stopped"
)

type HangoutTask struct {
	Config map[string]interface{}
	TaskId string
	RuleId string
	RuleName string
	Worker int
	ExitWhenNil bool
	inputs GoHangoutInputs
	configChannel chan map[string]interface{}
	AutoReload bool
	FileName string
	Base64Config string
	TaskStatus RunningStatus
	Vendor string
	Description string
	MainThreadExitChan chan struct{}
	CreateTime string
}

//任务启动参数
type TskStartParam struct {
	base64Config *string
	config map[string]interface{}
	fileName string
	commitChan chan TskActionResult
	ruleLoadMode common.RuleLoadMode
	StartInstant bool
}


func (hangoutTask *HangoutTask) Exit() {
	hangoutTask.MainThreadExitChan <- struct{}{}
}

func buildPluginLink(config map[string]interface{}, exitWhenNil bool, mainThreadExitChan chan struct{}) (boxes []*input.InputBox, err error) {
	boxes = make([]*input.InputBox, 0)
	for inputIdx, inputI := range config["inputs"].([]interface{}) {
		var inputPlugin topology.Input

		i := inputI.(map[interface{}]interface{})
		glog.Infof("input[%d] %v", inputIdx+1, i)

		// len(i) is 1
		for inputTypeI, inputConfigI := range i {
			inputType := inputTypeI.(string)
			inputConfig := inputConfigI.(map[interface{}]interface{})

			inputPlugin = input.GetInput(inputType, inputConfig)
			if inputPlugin == nil {
				err = fmt.Errorf("invalid input plugin")
				return
			}

			box := input.NewInputBox(inputPlugin, inputConfig, config, mainThreadExitChan)
			if box == nil {
				err = fmt.Errorf("new input box fail")
				return
			}
			box.SetShutdownWhenNil(exitWhenNil)
			boxes = append(boxes, box)
		}
	}
	return
}

func (inputs GoHangoutInputs) start(worker int) {
	boxes := ([]*input.InputBox)(inputs)
	var wg sync.WaitGroup
	wg.Add(len(boxes))

	for i := range boxes {
		go func(i int) {
			defer wg.Done()
			boxes[i].Beat(worker)
		}(i)
	}
	wg.Wait()
}

func (inputs GoHangoutInputs) Stop() {
	boxes := ([]*input.InputBox)(inputs)
	for _, box := range boxes {
		box.Shutdown()
	}
}

func (hangoutTask *HangoutTask) startInputs()  {
	inputs := hangoutTask.inputs
	go inputs.start(hangoutTask.Worker)
	configChannel := hangoutTask.configChannel
	go func() {
		for cfg := range configChannel {
			inputs.Stop()
			boxes, err := buildPluginLink(cfg, hangoutTask.ExitWhenNil, hangoutTask.MainThreadExitChan)
			if err == nil {
				inputs = boxes
				go inputs.start(hangoutTask.Worker)
			} else {
				glog.Errorf("build plugin link error: %v", err)
				hangoutTask.Exit()
			}
		}
	}()
	if hangoutTask.AutoReload && hangoutTask.FileName != "" {
		if err := watchConfig(hangoutTask.FileName, configChannel); err != nil {
			glog.Fatalf("watch config fail: %s", err)
		}
	}
	go listenSignal(inputs, configChannel, hangoutTask)
}

func (hangoutTask *HangoutTask) startTask(param *TskStartParam)  {
	configChannel := make(chan map[string]interface{})
    hangoutTask.configChannel = configChannel
	commitResult := TskActionResult{Success: false}
	commitChan := param.commitChan
	defer func() {
		commitChan <- commitResult
	}()
	boxes, err := buildPluginLink(hangoutTask.Config, hangoutTask.ExitWhenNil, hangoutTask.MainThreadExitChan)
	if err != nil {
		glog.Errorf("build plugin link error: %v", err)
		commitResult.Err = err
		return
	}
	inputs := GoHangoutInputs(boxes)
	hangoutTask.inputs = inputs
	if param.StartInstant {
		hangoutTask.startInputs()
	}
	commitResult.Success = true
}

func (tskManager *TskManager) restartTask(oldTask *HangoutTask, startInstant bool) (*TskActionResult, *HangoutTask) {
	if taskIsRunning(oldTask) {
		commitResult := &TskActionResult{Success: true}
		commitResult.TaskId = oldTask.TaskId
		glog.Infof("task with task id=%s has been running", oldTask.TaskId)
		return commitResult, oldTask
	}
	param := &TskStartParam{
		config: oldTask.Config,
		commitChan: make(chan TskActionResult),
		ruleLoadMode: common.Rpc,
		StartInstant: startInstant,
		base64Config: &oldTask.Base64Config,
	}
	commitResult := &TskActionResult{Success: false}
	hangoutTask, err := GenerateHangoutTask(param, tskManager)
	if err != nil {
		commitResult.Err = err
		return commitResult, oldTask
	}
	commitResult = taskManager.startHangoutTask(param, hangoutTask)
	if commitResult.Success {
		oldTask.stopTask()
	}
	return commitResult, hangoutTask
}

func (hangoutTask *HangoutTask) stopTask() {
	if hangoutTask.inputs != nil {
		glog.Infof("stop task with task_id=%s,rule_name=%s", hangoutTask.TaskId, hangoutTask.RuleName)
		hangoutTask.inputs.Stop()
	}
}

