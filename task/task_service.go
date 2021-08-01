package task

import (
    "github.com/kevinu2/gohangout/db"
)

type ITaskService interface {
    saveOrUpdateTask(task *RuleTask) error
    updateTaskStatus(taskId string, status int16) error
    queryTaskByShard(shard string) (error, []*RuleTask)
    fetchTaskById(taskId string) (error, *RuleTask)
    deleteTaskById(taskId string) error
}

var taskDbService ITaskService = new(RuleTaskServiceImpl)

type RuleTaskServiceImpl struct {}

func (t *RuleTaskServiceImpl) deleteTaskById(taskId string) error {
    ruleTask := new(RuleTask)
    ruleTask.Id = taskId
    return db.OrmDB.Delete(ruleTask).Error
}

func (t *RuleTaskServiceImpl) queryTaskByShard(shard string) (error, []*RuleTask) {
    ruleTasks := make([]*RuleTask, 0, 12)
    err := db.OrmDB.Find(&ruleTasks, RuleTask{Shard: shard}).Error
    return err, ruleTasks
}

func (t *RuleTaskServiceImpl) fetchTaskById(taskId string) (error, *RuleTask) {
    ruleTask := new(RuleTask)
    err := db.OrmDB.First(&ruleTask, RuleTask{Id: taskId}).Error
    return err, ruleTask
}

func (t *RuleTaskServiceImpl) saveOrUpdateTask(task *RuleTask) error {
    err := db.OrmDB.Save(task).Error
    return err
}

func (t *RuleTaskServiceImpl) updateTaskStatus(taskId string, status int16) error {
    ruleTask := new(RuleTask)
    ruleTask.Id = taskId
    return db.OrmDB.Model(ruleTask).Updates(RuleTask{Status: status}).Error
}

