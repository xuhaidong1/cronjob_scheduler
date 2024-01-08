package cronjob_scheduler

import (
	"context"
	"github.com/google/uuid"
	"github.com/xuhaidong1/cronjob_scheduler/domain"
	"github.com/xuhaidong1/cronjob_scheduler/executor"
	"github.com/xuhaidong1/cronjob_scheduler/internal"
	"github.com/xuhaidong1/go-generic-tools/pluginsx/logx"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"gorm.io/gorm"
	"time"
)

func NewScheduler(db *gorm.DB, strategyType ScheduleStrategyType, preemptStrategyType PreemptStrategyType, opts ...ScheduleOption) *Scheduler {
	sc := defaultSchedulerConfig
	for _, opt := range opts {
		opt(&sc)
	}
	switch strategyType {
	case LongPreemptStrategyType:
		sc.ScheduleSg = NewLongPreemptStrategy()
	default:
		sc.ScheduleSg = NewOncePreemptStrategy()
	}
	switch preemptStrategyType {
	case LoadBalancerStrategyType:
		sc.PreemptSg = internal.NewLoadBalancerStrategy()
	default:
		sc.PreemptSg = internal.NewTimeoutPreemptStrategy(db, sc.TimeoutInterval)
	}
	dao := internal.NewGORMJobDAO(db, sc.PreemptSg)
	repo := internal.NewCronJobRepository(dao)
	svc := internal.NewCronJobService(repo, sc.RefreshInterval, sc.Logger)
	scr := newScheduler(svc, sc)

	scr.RegisterExecutor(executor.NewLocalFuncExecutor())
	scr.RegisterExecutor(executor.NewHttpExecutor())
	go func() {
		err := scr.Schedule(context.Background())
		if err != nil {
			scr.l.Error("schedule err", logx.Error(err))
		}
	}()
	return scr
}

func (s *Scheduler) RegisterJob(ctx context.Context, name string, timeout time.Duration, cron string, f func(context.Context) error) error {
	s.execs[executor.ExecutorTypeLocalFunc].RegisterRunner(name, s.JobWrapper(f))
	return s.svc.Register(ctx, domain.Job{
		Name:     name,
		Timeout:  timeout,
		Cron:     cron,
		Executor: string(executor.ExecutorTypeLocalFunc),
	})
}

type ScheduleOption func(*SchedulerConfig)

type SchedulerConfig struct {
	//续约任务的时间间隔，RefreshInterval要略小于TimeoutInterval
	RefreshInterval time.Duration
	//如果一个在running的任务超过TimeoutInterval没有人续约，则认为这个任务续约失败，可以被剥夺
	TimeoutInterval time.Duration
	//抢占的最多的任务并发数，如果抢占了超过MaxConcurrentPreemptNum数量的任务，调度器会阻塞，
	//直到所占有的任务数量小于MaxConcurrentPreemptNum才会继续抢占
	MaxConcurrentPreemptNum int
	//调度策略；分为长期抢占的循环调度，和单次调度，默认为单次调度
	ScheduleSg ScheduleStrategy
	// 抢占策略：分为负载均衡抢占策略和普通抢占策略；负载均衡策略：执行任务时，如果自身处于高负载状态且有低负载候选者，
	// 会把任务让出去，让低负载的候选者抢占；两种策略均有续约和抢占机制
	PreemptSg internal.PreemptStrategy
	Logger    logx.Logger
}

var defaultSchedulerConfig = SchedulerConfig{
	RefreshInterval:         time.Second * 50,
	TimeoutInterval:         time.Minute,
	MaxConcurrentPreemptNum: 100,
	Logger: func() logx.Logger {
		log, _ := logx.NewLogger(zap.InfoLevel).Build()
		return logx.NewZapLogger(log)
	}(),
}

func WithRefreshInterval(interval time.Duration) ScheduleOption {
	return func(c *SchedulerConfig) {
		c.RefreshInterval = interval
	}
}
func WithTimeoutInterval(interval time.Duration) ScheduleOption {
	return func(c *SchedulerConfig) {
		c.TimeoutInterval = interval
	}
}
func WithMaxConcurrentPreemptNum(num int) ScheduleOption {
	return func(c *SchedulerConfig) {
		c.MaxConcurrentPreemptNum = num
	}
}

// Scheduler 调度器
type Scheduler struct {
	Name       string
	execs      map[executor.ExecutorType]executor.Executor
	svc        internal.JobService
	l          logx.Logger
	limiter    *semaphore.Weighted
	ScheduleSg ScheduleStrategy
}

func (s *Scheduler) RegisterExecutor(exec executor.Executor) {
	s.execs[exec.Name()] = exec
}

func (s *Scheduler) Schedule(ctx context.Context) error {
	//每0.5s去数据库抢占1次
	tk := time.NewTicker(time.Millisecond * 500)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.DeadlineExceeded
		case <-tk.C:
			// 一次调度的数据库查询时间
			//dbCtx, cancel := context.WithTimeout(ctx, time.Millisecond*400)
			job, err := s.svc.Preempt(ctx, s.Name)
			//  cancel()

			switch err {
			case context.DeadlineExceeded:
				s.l.Warn("抢占操作超时", logx.Error(err))
				continue
			case gorm.ErrRecordNotFound:
				continue
			case nil:
				er := s.limiter.Acquire(ctx, 1)
				if er != nil {
					return er
				}
			default:
				s.l.Error("抢占出现错误", logx.Error(err))
				return err
			}

			// 接下来就是执行
			// 怎么执行？--异步执行，不要阻塞主调度循环
			go func(j domain.Job) {
				defer func() {
					s.limiter.Release(1)
					er := j.CancelFunc()
					if er != nil {
						s.l.Error("释放任务失败", logx.Error(er), logx.Int64("jobID", j.Id))
					}
				}()
				exec, ok := s.execs[executor.ExecutorType(j.Executor)]
				if !ok {
					s.l.Error("未找到对应的执行器",
						logx.String("executor", string(j.Executor)))
					return
				}
				for {
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
					// 如果有下一次执行（长抢占），则等待下一次调度，否则释放任务
					duration, hasNext := s.ScheduleSg.Next(j)
					if !hasNext {
						return
					}
					time.Sleep(duration)
				}
			}(job)
		}
	}
}

func (s *Scheduler) JobWrapper(f func(ctx context.Context) error) executor.Func {
	return func(ctx context.Context, j domain.Job) error {
		start := time.Now()
		s.l.Info("开始执行job", logx.String("name", j.Name), logx.String("scheduler:", s.Name))
		defer func() {
			end := time.Now()
			s.l.Info("job执行完成", logx.String("name", j.Name), logx.String("耗时", end.Sub(start).String()), logx.String("scheduler:", s.Name))
		}()
		return f(ctx)
	}
}

func newScheduler(svc internal.JobService, sc SchedulerConfig) *Scheduler {
	return &Scheduler{
		Name:       uuid.New().String(),
		svc:        svc,
		l:          sc.Logger,
		limiter:    semaphore.NewWeighted(int64(sc.MaxConcurrentPreemptNum)),
		ScheduleSg: sc.ScheduleSg,
		execs:      make(map[executor.ExecutorType]executor.Executor)}
}
