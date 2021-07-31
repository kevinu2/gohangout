package rpc

import (
    "context"
    "github.com/golang/glog"
    "github.com/kevinu2/gohangout/common"
    "github.com/kevinu2/gohangout/task"
)

var (
    taskManager = task.GetTaskManager()
)

type EtlRule struct {}

type UploadReply struct {
    Status common.ResponseCode
    ErrMsg string
    TaskId string
    TaskShard string
    Vendor string
    RuleId string
}

func (etlRule *EtlRule) Upload(ctx context.Context, args *common.UploadArgs, reply *UploadReply) error {
    reply.Status = common.FAIL
    commitResult := taskManager.UploadRuleTask(args)
    reply.Vendor = commitResult.Vendor
    reply.RuleId = commitResult.RuleId
    reply.TaskShard = commitResult.TaskShard
    if commitResult.Success {
        reply.Status = common.SUCCESS
        reply.TaskId = commitResult.TaskId
        return nil
    }
    if commitResult.Err != nil {
        glog.Error(commitResult.Err)
        reply.ErrMsg = commitResult.Err.Error()
    }
    return nil
}

