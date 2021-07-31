package task

import (
    "github.com/golang/glog"
    "github.com/kevinu2/gohangout/db"
    "time"
)

type RuleTask struct {
    Id       string `gorm:"column:id;PRIMARY_KEY;type:varchar(64);not null"`
    RuleId   string `gorm:"column:rule_id;type:varchar(64);not null;unique_index:uk_hangout_task_rule_shard_vendor"`
    RuleName  string `gorm:"column:rule_name;type:varchar(256)"`
    Shard     string `gorm:"column:shard;type:varchar(256);unique_index:uk_hangout_task_rule_shard_vendor"`
    Vendor    string  `gorm:"column:vendor;type:varchar(64);default 'hangout';unique_index:uk_hangout_task_rule_shard_vendor"`
    Description   string `gorm:"column:description;type:varchar(1024)"`
    Status       int16 `gorm:"column:status;type:int2;default 0"`
    RuleContent  string  `gorm:"column:rule_content;type:text;not null"`
    CreatedAt   time.Time `gorm:"column:create_time;autoCreateTime;type:timestamp"`
    UpdatedAt  time.Time `gorm:"column:update_time;autoUpdateTime;type:timestamp"`
}

func (v *RuleTask) TableName() string {
    return "hangout_task"
}

func CreateRuleTaskTable() {
    if !db.OrmDB.HasTable(&RuleTask{}) {
       err := db.OrmDB.CreateTable(&RuleTask{}).Error
       if err != nil {
           glog.Fatal(err)
       }
    }
}