package cronjob_scheduler

import (
	"github.com/xuhaidong1/cronjob_scheduler/domain"
	"time"
)

type ScheduleStrategy interface {
	Next(j domain.Job) (time.Duration, bool)
}

// LongScheduleStrategy 长期抢占,抢到了任务就不释放，除非自身续约失败/负载较高被其它实例剥夺任务
type LongScheduleStrategy struct {
}

func NewLongScheduleStrategy() ScheduleStrategy {
	return &LongScheduleStrategy{}
}

func (s LongScheduleStrategy) Next(j domain.Job) (time.Duration, bool) {
	next := j.NextTime()
	if next.IsZero() {
		return 0, false
	}
	return next.Sub(time.Now()), true
}

// OnceScheduleStrategy 抢到了任务只执行一次，执行完之后就释放任务
type OnceScheduleStrategy struct {
}

func NewOnceScheduleStrategy() ScheduleStrategy {
	return &OnceScheduleStrategy{}
}

func (s OnceScheduleStrategy) Next(j domain.Job) (time.Duration, bool) {
	return 0, false
}
