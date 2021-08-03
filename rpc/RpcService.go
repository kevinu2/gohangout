package rpc

import (
    "context"
    "github.com/golang/glog"
    "github.com/kevinu2/gohangout/task"
)

var (
    taskManager = task.GetTaskManager()
)

type HangoutTask struct {}

type ResponseCode string
const (
    SUCCESS ResponseCode = "0"
    FAIL    ResponseCode = "1"
)

type RequestAction int16
const (
    SetAction RequestAction = 1 //upload
    LoadAction RequestAction = 2 //start
    OffloadAction RequestAction = 3 //stop
    ResetAction RequestAction = 4 // delete->noupload,
    StatusAction RequestAction = 5
    GetAction RequestAction = 6
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

type EmptyArgs struct {}

type SimpleReply struct {
    RespCode ResponseCode
    ErrMsg string
    TaskStatus task.RunningStatus
}

type Reply struct {
    SimpleReply
    TaskId string
    Vendor string
    RuleId string
    RuleName string
    RuleDesc string
    RuleContent *string
}

type StatusReply struct {
    SimpleReply
}

type GetReply struct {
    SimpleReply
    RuleContent *string
    TaskId string
    Vendor string
    RuleId string
    RuleName string
    RuleDesc string
}

func handleResponseResult(reply *Reply, actionResult *task.TskActionResult)  {
    reply.RespCode = FAIL
    reply.TaskId = actionResult.TaskId
    reply.Vendor = actionResult.Vendor
    reply.RuleId = actionResult.RuleId
    reply.RuleContent = &actionResult.Base64Config
    reply.TaskStatus = actionResult.TaskStatus
    reply.RuleDesc = actionResult.RuleDesc
    reply.RuleName = actionResult.RuleName
    glog.Infof("current task status:%s", actionResult.TaskStatus)
    if actionResult.Success {
        reply.RespCode = SUCCESS
    }
    if actionResult.Err != nil {
        glog.Error(actionResult.Err)
        reply.ErrMsg = actionResult.Err.Error()
    }
}

func executeRpcAction(reply *Reply, action RequestAction)  {
    reply.RespCode = SUCCESS
    var commitResult *task.TskActionResult
    switch action {
    case LoadAction:
        commitResult = taskManager.LoadRpcTask()
    case OffloadAction:
        commitResult = taskManager.OffloadRpcTask()
    case ResetAction:
        commitResult = taskManager.ResetRpcTask()
    case StatusAction:
        commitResult = taskManager.StatusRpcTask()
    case GetAction:
        commitResult = taskManager.GetRpcTask()
    }
    handleResponseResult(reply, commitResult)
}

// set操作
func (hangoutTask *HangoutTask) Set(ctx context.Context, args *UploadArgs, reply *SimpleReply) error {
    request := &task.TskRpcRequest{UploadContent: args.RuleContent}
    actionResult := taskManager.UploadRpcTask(request)
    tmpReply := new(Reply)
    handleResponseResult(tmpReply, actionResult)
    reply.RespCode = tmpReply.RespCode
    reply.ErrMsg = tmpReply.ErrMsg
    reply.TaskStatus = tmpReply.TaskStatus
    return nil
}


func getSimpleReplyResult(action RequestAction, reply *SimpleReply) {
    tmpReply := new(Reply)
    executeRpcAction(tmpReply, action)
    reply.RespCode = tmpReply.RespCode
    reply.ErrMsg = tmpReply.ErrMsg
    reply.TaskStatus = tmpReply.TaskStatus
}

func getStatusReplyResult(action RequestAction, reply *StatusReply) {
    tmpReply := new(Reply)
    executeRpcAction(tmpReply, action)
    reply.RespCode = tmpReply.RespCode
    reply.ErrMsg = tmpReply.ErrMsg
    reply.TaskStatus = tmpReply.TaskStatus
}

func getGetReplyResult(action RequestAction, reply *GetReply) {
    tmpReply := new(Reply)
    executeRpcAction(tmpReply, action)
    reply.RespCode = tmpReply.RespCode
    reply.RuleId = tmpReply.RuleId
    reply.ErrMsg = tmpReply.ErrMsg
    reply.TaskStatus = tmpReply.TaskStatus
    reply.RuleContent = tmpReply.RuleContent
    reply.RuleName = tmpReply.RuleName
    reply.RuleDesc = tmpReply.RuleDesc
    reply.TaskId = tmpReply.TaskId
    reply.Vendor = tmpReply.Vendor
}

//Load相当于start操作
func (hangoutTask *HangoutTask) Load(ctx context.Context, args *EmptyArgs, reply *SimpleReply) error {
    getSimpleReplyResult(LoadAction, reply)
    return nil
}

//Offload相当于stop操作
func (hangoutTask *HangoutTask) Offload(ctx context.Context, args *EmptyArgs, reply *SimpleReply) error {
    getSimpleReplyResult(OffloadAction, reply)
    return nil
}

func (hangoutTask *HangoutTask) Status(ctx context.Context, args *EmptyArgs, reply *StatusReply) error {
    getStatusReplyResult(StatusAction, reply)
    return nil
}

func (hangoutTask *HangoutTask) Reset(ctx context.Context, args *EmptyArgs, reply *SimpleReply) error {
    getSimpleReplyResult(ResetAction, reply)
    return nil
}

func (hangoutTask *HangoutTask) Get(ctx context.Context, args *EmptyArgs, reply *GetReply) error {
    getGetReplyResult(GetAction, reply)
    return nil
}