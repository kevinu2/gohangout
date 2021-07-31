package task

import (
    "github.com/kevinu2/gohangout/db"
)

type ITaskService interface {
    saveTask(task *RuleTask) error
    updateTask(task *RuleTask) error
    queryTaskByShard(shard string) (error, []*RuleTask)
}

var taskDbService ITaskService = new(RuleTaskServiceImpl)

type RuleTaskServiceImpl struct {}

func (t *RuleTaskServiceImpl) queryTaskByShard(shard string) (error, []*RuleTask) {
    ruleTasks := make([]*RuleTask, 0, 12)
    err := db.OrmDB.Find(&ruleTasks, RuleTask{Shard: shard}).Error
    return err, ruleTasks
}

func (t *RuleTaskServiceImpl) saveTask(task *RuleTask) error {
    err := db.OrmDB.Save(task).Error
    return err
}

func (t *RuleTaskServiceImpl) updateTask(task *RuleTask) error {
    panic("implement me")
}

