package ioc

import (
	"github.com/xuhaidong1/cronjob_scheduler/internal"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"os"
	"sync"
	"time"
)

var (
	db         *gorm.DB
	dbInitOnce sync.Once
)

func InitDB() *gorm.DB {
	dsn := "root:root@tcp(localhost:13346)/cronjob"
	var err error
	dbInitOnce.Do(func() {
		db, err = gorm.Open(mysql.Open(dsn),
			&gorm.Config{
				Logger: logger.New(
					log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
					logger.Config{
						SlowThreshold:             time.Millisecond * 100, // Slow SQL threshold
						LogLevel:                  logger.Silent,          // Log level
						IgnoreRecordNotFoundError: true,                   // Ignore ErrRecordNotFound error for logger
						ParameterizedQueries:      true,                   // Don't include params in the SQL log
						Colorful:                  false,                  // Disable color
					},
				),
			})
		if err != nil {
			panic(err)
		}
		//initTables(db)
	})
	return db
}

func InitTables(db *gorm.DB) {
	err := internal.InitTable(db)
	if err != nil {
		panic(err)
	}
}
