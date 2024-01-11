package cronjob_scheduler

import (
	"github.com/xuhaidong1/cronjob_scheduler/domain"
	"math"
	"sync"
	"time"
)

// DownGradeStrategy 业务高峰期时的策略
type DownGradeStrategy interface {
	// Do 降级来时进行job的释放
	Do(scr *Scheduler)
}

type RunNoJobStrategy struct {
}

func NewRunNoJobStrategy() DownGradeStrategy {
	return &RunNoJobStrategy{}
}

func (r RunNoJobStrategy) Do(scr *Scheduler) {
	scr.RunningJobs.Range(func(id, job any) bool {
		j := job.(domain.Job)
		j.Cancel()
		return true
	})
	time.Sleep(time.Second)
	scr.RunningJobs = &sync.Map{}
}

type LimitLoad struct {
	GiveUpStrategy
	Threshold int64
}

type GiveUpStrategy string

const (
	LowWeightFirst  GiveUpStrategy = "lowWeight"
	HighWeightFirst GiveUpStrategy = "highWeight"
)

// NewLimitLoad 非负载均衡也能用
func NewLimitLoad(g GiveUpStrategy, threshold int64) DownGradeStrategy {
	return &LimitLoad{g, threshold}
}

func (l LimitLoad) Do(scr *Scheduler) {
	switch l.GiveUpStrategy {
	case LowWeightFirst:
		for scr.GetLoad() > l.Threshold {
			minWeightJob := domain.Job{Id: -1, Weight: math.MaxInt64}
			scr.RunningJobs.Range(func(id, job any) bool {
				j := job.(domain.Job)
				if j.Weight < minWeightJob.Weight {
					minWeightJob = j
				}
				return true
			})
			if minWeightJob.Id != -1 {
				minWeightJob.Cancel()
				time.Sleep(time.Second)
			}
		}
	case HighWeightFirst:
		for scr.GetLoad() > l.Threshold {
			maxWeightJob := domain.Job{Id: -1, Weight: 0}
			scr.RunningJobs.Range(func(id, job any) bool {
				j := job.(domain.Job)
				if j.Weight > maxWeightJob.Weight {
					maxWeightJob = j
				}
				return true
			})
			if maxWeightJob.Id != -1 {
				maxWeightJob.Cancel()
				time.Sleep(time.Second)
			}
		}
	}
}

type GiveUpNotImportantStrategy struct {
}

func (g GiveUpNotImportantStrategy) Do() {
	panic("implement me")
	// job加个tag即可
}
