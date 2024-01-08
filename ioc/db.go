package ioc

import (
	"github.com/xuhaidong1/cronjob_scheduler/internal"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"sync"
)

var (
	db         *gorm.DB
	dbInitOnce sync.Once
)

func InitDB() *gorm.DB {
	dsn := "root:root@tcp(localhost:13346)/cronjob"
	var err error
	dbInitOnce.Do(func() {
		db, err = gorm.Open(mysql.Open(dsn))
		if err != nil {
			panic(err)
		}
		initTables(db)
	})
	return db
}

func initTables(db *gorm.DB) {
	err := internal.InitTable(db)
	if err != nil {
		panic(err)
	}
}
