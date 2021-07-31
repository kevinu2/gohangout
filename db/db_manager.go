package db

import (
	"database/sql"
	"fmt"
	"github.com/golang/glog"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/kevinu2/gohangout/cfg"
	"time"
)

var (
	SqlDB *sql.DB
	OrmDB *gorm.DB
)

func InitDb() {
	dbConfig := cfg.GetAppConfig().DbConfig
	pgArgs := fmt.Sprintf("host=%v port=%v user=%v dbname=%v password=%v sslmode=disable",
		dbConfig.Host, dbConfig.Port, dbConfig.User, dbConfig.Db, dbConfig.Password)
	db, err := gorm.Open("postgres", pgArgs)
	if err != nil {
		glog.Fatal("Connect db error:", err)
	}
	OrmDB = db
	SqlDB = OrmDB.DB()
	// SetMaxIdleConns 设置空闲连接池中连接的最大数量
	SqlDB.SetMaxIdleConns(5)
	// SetMaxOpenConns 设置打开数据库连接的最大数量。
	SqlDB.SetMaxOpenConns(15)
	// SetConnMaxLifetime 设置了连接可复用的最大时间。
	SqlDB.SetConnMaxLifetime(time.Minute * 20)
}

func Close() {
	if OrmDB != nil {
		_ = OrmDB.Close()
	}
	if SqlDB != nil {
		_ = SqlDB.Close()
	}
}
