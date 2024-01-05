package cronjob_scheduler

import (
	"context"
	"github.com/xuhaidong1/cronjob_scheduler/executor"
	"github.com/xuhaidong1/cronjob_scheduler/internal"
	"github.com/xuhaidong1/go-generic-tools/pluginsx/logx"
	"golang.org/x/sync/semaphore"
	"gorm.io/gorm"
	"time"
)

// Scheduler 调度器
type Scheduler struct {
	execs   map[executor.ExecutorType]executor.Executor
	svc     internal.JobService
	l       logx.Logger
	limiter *semaphore.Weighted
}

type ScheduleStrategyType int

const (
	MutexStategy ScheduleStrategyType = iota
	Preemptive
	LoadBalancePreemptive
)

func NewScheduler(svc internal.JobService, MaxPreemptNum int, l logx.Logger) *Scheduler {
	return &Scheduler{svc: svc, l: l,
		limiter: semaphore.NewWeighted(int64(MaxPreemptNum)),
		execs:   make(map[executor.ExecutorType]executor.Executor)}
}

func (s *Scheduler) RegisterExecutor(exec executor.Executor) {
	s.execs[exec.Name()] = exec
}

func (s *Scheduler) Schedule(ctx context.Context) error {
	for {

		if ctx.Err() != nil {
			// 退出调度循环
			return ctx.Err()
		}
		err := s.limiter.Acquire(ctx, 1)
		if err != nil {
			return err
		}
		// 一次调度的数据库查询时间
		dbCtx, cancel := context.WithTimeout(ctx, time.Second)
		j, err := s.svc.Preempt(dbCtx)
		cancel()

		switch err {
		case context.DeadlineExceeded:
			s.l.Warn("抢占操作超时", logx.Error(err))
			continue
		case gorm.ErrRecordNotFound:
			continue
		case nil:
		default:
			s.l.Error("抢占出现错误", logx.Error(err))
			return err
		}

		exec, ok := s.execs[executor.ExecutorType(j.Executor)]
		if !ok {
			s.l.Error("未找到对应的执行器",
				logx.String("executor", string(j.Executor)))
			continue
		}

		// 接下来就是执行
		// 怎么执行？--异步执行，不要阻塞主调度循环
		go func() {
			defer func() {
				s.limiter.Release(1)
				er := j.CancelFunc()
				if er != nil {
					s.l.Error("释放任务失败", logx.Error(er), logx.Int64("jobID", j.Id))
				}
			}()

			// 考虑任务的超时控制
			ctx1, cancel1 := context.WithTimeout(ctx, j.Timeout)
			err1 := exec.Exec(ctx1, j)
			cancel1()
			if err1 != nil {
				s.l.Error("任务执行错误", logx.Error(err1))
			}

			// 执行完成，设定下一次调度时间
			ctx2, cancel2 := context.WithTimeout(ctx, time.Second)
			err1 = s.svc.SetNextTime(ctx2, j)
			cancel2()
			if err1 != nil {
				s.l.Error("设置下一次执行时间失败", logx.Int64("jobID", j.Id), logx.Error(err1))
			}
		}()
	}
}
