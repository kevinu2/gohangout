package task

import (
    "errors"
    "github.com/kevinu2/gohangout/common"
)

//只上传任务,构造出inputs,不启动
func (tskManager *TskManager) UploadRpcTask(args *TskRpcRequest) *TskActionResult {
    task := readOneTaskFromCache()
    actionResult := &TskActionResult{
        Success: true,
    }
    if task != nil {
        actionResult.Success = false
        actionResult.TaskStatus = task.TaskStatus
        actionResult.Err = errors.New("task has been existed")
        return actionResult
    }
    param, err := GenerateStartParam(&args.UploadContent, common.Rpc, false)
    if err != nil {
        actionResult.Success = false
        actionResult.Err = err
        return actionResult
    }
    hangoutTask, err := GenerateHangoutTask(param, tskManager)
    if err != nil {
        actionResult.Success = false
        actionResult.Err = err
        return actionResult
    }
    commitResult := tskManager.startHangoutTask(param, hangoutTask)
    if commitResult.Success  {
        handleCommitResult(commitResult, hangoutTask)
        hangoutTask.TaskStatus =  Uploaded
        commitResult.TaskStatus = Uploaded
        commitResult.TaskId = hangoutTask.TaskId
    }
    return commitResult
}

//启动任务
func (tskManager *TskManager) LoadRpcTask() *TskActionResult {
    actionResult := &TskActionResult{
        Success: true,
    }
    task := readOneTaskFromCache()
    if task == nil {
        actionResult.TaskStatus = Unupload
        actionResult.Success = false
        actionResult.Err = errors.New("task not found")
        return actionResult
    }
    defer func() {
        actionResult.TaskStatus = task.TaskStatus
    }()
    commitResult, newTask := tskManager.restartTask(task, true)
    if !commitResult.Success {
        actionResult.Success = false
        actionResult.Err = commitResult.Err
        return actionResult
    }
    newStatus := Starting
    actionResult.RuleId = newTask.RuleId
    actionResult.Vendor = newTask.Vendor
    newTask.TaskStatus = newStatus
    actionResult.TaskId = newTask.TaskId
    handleCommitResult(commitResult, newTask)
    return actionResult
}

//停止任务
func (tskManager *TskManager) OffloadRpcTask() *TskActionResult {
    actionResult := &TskActionResult{
        Success: false,
    }
    task := readOneTaskFromCache()
    if task == nil {
        actionResult.Err = errors.New("task not found")
        actionResult.TaskStatus = Unupload
        return actionResult
    }
    taskManager.Lock()
    defer taskManager.Unlock()
    oldStatus := task.TaskStatus
    newStatus := Stopped
    task.TaskStatus = newStatus
    actionResult.RuleId = task.RuleId
    actionResult.Vendor = task.Vendor
    actionResult.TaskId = task.TaskId
    actionResult.TaskStatus = newStatus
    actionResult.Success = true
    if oldStatus == Starting {
        task.stopTask()
    }
    return actionResult
}

//删除
func (tskManager *TskManager) ResetRpcTask() *TskActionResult {
    actionResult := &TskActionResult{
        Success: true,
    }
    task := readOneTaskFromCache()
    defer func() {
        actionResult.TaskStatus = Unupload
    }()
    if task == nil {
        return actionResult
    }
    if task.TaskStatus == Starting {
        actionResult.Err = errors.New("task is running")
        actionResult.Success = false
        return actionResult
    }
    actionResult.RuleId = task.RuleId
    actionResult.Vendor = task.Vendor
    actionResult.TaskId = task.TaskId
    deleteTaskCache(task)
    return actionResult
}

func (tskManager *TskManager) StatusRpcTask() *TskActionResult {
    actionResult := &TskActionResult{
        Success: true,
    }
    task := readOneTaskFromCache()
    if task == nil {
        actionResult.TaskStatus = Unupload
        return actionResult
    }
    actionResult.TaskId = task.TaskId
    actionResult.RuleId = task.RuleId
    actionResult.Vendor = task.Vendor
    actionResult.TaskStatus = task.TaskStatus
    return actionResult
}

func (tskManager *TskManager) GetRpcTask() *TskActionResult {
    actionResult := &TskActionResult{
        Success: true,
    }
    task := readOneTaskFromCache()
    if task == nil {
        actionResult.TaskStatus = Unupload
        actionResult.Base64Config = ""
        return actionResult
    }
    actionResult.Base64Config = task.Base64Config
    actionResult.TaskId = task.TaskId
    actionResult.RuleId = task.RuleId
    actionResult.Vendor = task.Vendor
    actionResult.RuleName = task.RuleName
    actionResult.RuleDesc = task.Description
    actionResult.TaskStatus = task.TaskStatus
    return actionResult
}