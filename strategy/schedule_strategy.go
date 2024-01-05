package strategy

import (
	"github.com/xuhaidong1/cronjob_scheduler/domain"
	"time"
)

type ScheduleStrategy interface {
	Next(j domain.Job) (time.Duration, bool)
}

// LongPreemptStrategy 长期抢占,抢到了任务就不释放，除非自身续约失败/负载较高被其它实例剥夺任务
type LongPreemptStrategy struct {
}

func (s LongPreemptStrategy) Next(j domain.Job) (time.Duration, bool) {
	next := j.NextTime()
	if next.IsZero() {
		return 0, false
	}
	return next.Sub(time.Now()), true
}

// OncePreemptStrategy 抢到了任务只执行一次，执行完之后就释放任务
type OncePreemptStrategy struct {
}

func (s OncePreemptStrategy) Next(j domain.Job) (time.Duration, bool) {
	return 0, false
}

//type ScheduleStrategyType string
//
//const (
//	LongPreemptStrategyType ScheduleStrategyType = "long"
//	OncePreemptStrategyType ScheduleStrategyType = "once"
//)
