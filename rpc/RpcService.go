package rpc

import (
    "context"
    "github.com/golang/glog"
    "github.com/kevinu2/gohangout/task"
    "github.com/kevinu2/gohangout/utils"
)

var (
    taskManager = task.GetTaskManager()
)

type EtlTask struct {}

type ResponseCode string
const (
    SUCCESS ResponseCode = "0"
    FAIL    ResponseCode = "1"
)

type RequestAction int16
const (
    StartAction RequestAction = 1
    StopAction RequestAction = 2
    DeleteAction RequestAction = 3
)

type ErrorItem struct {
    Msg string
    TaskId string
    TaskShard string
    Vendor string
    RuleId string
}

type UploadArgs struct {
    //base64编码
    RuleContent string
}

type SingleReply struct {
    Status ResponseCode
    ErrMsg string
    TaskId string
    TaskShard string
    Vendor string
    RuleId string
}

type Reply struct {
    Status ResponseCode
    Errors []*ErrorItem
}

type TaskItem struct {
    TaskId string
    Vendor string
    RuleId string
}

type Args struct {
    TaskItems []TaskItem
}

func handleResponseResult(reply *SingleReply, actionResult *task.TskActionResult) error {
    reply.Status = FAIL
    reply.TaskId = actionResult.TaskId
    reply.Vendor = actionResult.Vendor
    reply.RuleId = actionResult.RuleId
    reply.TaskShard = actionResult.TaskShard
    if actionResult.Success {
        reply.Status = SUCCESS
        return nil
    }
    if actionResult.Err != nil {
        glog.Error(actionResult.Err)
        reply.ErrMsg = actionResult.Err.Error()
    }
    return nil
}

func createErrorItem(reply *SingleReply) *ErrorItem {
    item := new(ErrorItem)
    item.TaskId = reply.TaskId
    item.Vendor = reply.Vendor
    item.RuleId = reply.RuleId
    item.TaskShard = reply.TaskShard
    return item
}

func getTaskIds(args *Args) []string {
    taskIds := make([]string, 0)
    taskItems := args.TaskItems
    if len(taskItems) == 0 {
        return taskIds
    }
    for _, item := range taskItems {
        if !utils.StrIsEmpty(item.TaskId) {
            taskIds = append(taskIds, item.TaskId)
        } else {
            taskId := task.GetTaskId(item.Vendor, item.RuleId)
            taskIds = append(taskIds, taskId)
        }
    }
    return taskIds
}

func executeRpcAction(args *Args, reply *Reply, action RequestAction) error {
    reply.Status = SUCCESS
    taskIds := getTaskIds(args)
    replyErrors := make([]*ErrorItem, 0)
    if len(taskIds) == 0 {
        reply.Errors = replyErrors
        return nil
    }
    var commitResult *task.TskActionResult
    for _, taskId := range taskIds {
        request := &task.TskRpcRequest{TaskId: taskId}
        switch action {
        case StartAction:
            commitResult = taskManager.StartOrKillRpcTask(request, false)
        case StopAction:
            commitResult = taskManager.StartOrKillRpcTask(request, true)
        case DeleteAction:
            commitResult = taskManager.DeleteRpcTask(request)
        }
        singleReply := new(SingleReply)
        _ = handleResponseResult(singleReply, commitResult)
        if singleReply.Status != SUCCESS {
            reply.Status = FAIL
            item := createErrorItem(singleReply)
            replyErrors = append(replyErrors, item)
        }
    }
    reply.Errors = replyErrors
    return nil
}

func (etlTask *EtlTask) Upload(ctx context.Context, args *UploadArgs, reply *SingleReply) error {
    request := &task.TskRpcRequest{UploadContent: args.RuleContent}
    actionResult := taskManager.UploadRpcTask(request)
    _ = handleResponseResult(reply, actionResult)
    return nil
}

func (etlTask *EtlTask) Delete(ctx context.Context, args *Args, reply *Reply) error {
    return executeRpcAction(args, reply, DeleteAction)
}

func (etlTask *EtlTask) Stop(ctx context.Context, args *Args, reply *Reply) error {
    return executeRpcAction(args, reply, StopAction)
}

func (etlTask *EtlTask) Start(ctx context.Context, args *Args, reply *Reply) error {
    return executeRpcAction(args, reply, StartAction)
}

