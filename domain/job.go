package domain

import (
	"github.com/robfig/cron/v3"
	"time"
)

type Job struct {
	Id   int64
	Name string
	//执行的超时时间
	Timeout time.Duration
	Cron    string
	//执行流水版本号，用于乐观锁控制
	Version int64
	//执行器名字
	Executor string
	// 不知道配置具体细节，所以就搞一个通用的配置抽象
	Config     string
	CancelFunc func() error
}

var parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom |
	cron.Month | cron.Dow | cron.Descriptor)

func (j Job) NextTime() time.Time {
	// 根据 cron 表达式来算
	s, _ := parser.Parse(j.Cron)
	return s.Next(time.Now())
}
