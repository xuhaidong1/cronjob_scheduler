package cronjob_scheduler

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/xuhaidong1/cronjob_scheduler/domain"
	"github.com/xuhaidong1/cronjob_scheduler/executor"
	"github.com/xuhaidong1/cronjob_scheduler/internal"
	"github.com/xuhaidong1/go-generic-tools/pluginsx/logx"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"gorm.io/gorm"
	"sort"
	"sync/atomic"
	"time"
)

func NewScheduler(db *gorm.DB, opts ...ScheduleOption) *Scheduler {
	sc := defaultSchedulerConfig
	sc.DB = db
	//默认单次调度策略，非负载均衡的抢占策略，如果指定其它策略会再option应用环节替换掉
	sc.ScheduleSg = NewOnceScheduleStrategy()
	sc.PreemptSg = internal.NewTimeoutPreemptStrategy(db, sc.TimeoutInterval)
	//由于希望按顺序调用option函数，比如负载均衡抢占策略需要用到超时时间，如果用户指定了超时时间
	//那么这个超时时间同样也要再负载均衡抢占策略中初始化好。
	sort.Slice(opts, func(i, j int) bool {
		return opts[i].Idx < opts[j].Idx
	})
	for _, opt := range opts {
		opt.F(&sc)
	}
	dao := internal.NewGORMJobDAO(db, sc.PreemptSg)
	repo := internal.NewCronJobRepository(dao)
	svc := internal.NewCronJobService(repo, sc.Logger)
	scr := newScheduler(svc, sc)
	scr.RegisterExecutor(executor.NewLocalFuncExecutor())
	scr.RegisterExecutor(executor.NewHttpExecutor())
	_ = scr.svc.RegisterScheduler(context.Background(), scr.Name)
	return scr
}

func (s *Scheduler) Run() {
	go func() {
		err := s.RefreshSelf(context.Background())
		if err != nil {
			s.l.Error("scheduler refresh self err", logx.Error(err))
		}
	}()
	go func() {
		err := s.Schedule(context.Background())
		if err != nil {
			s.l.Error("schedule err", logx.Error(err))
		}
	}()
}

func (s *Scheduler) RegisterJob(name string, f func(context.Context) error) {
	s.execs[executor.ExecutorTypeLocalFunc].RegisterRunner(name, s.JobWrapper(f))
}

func (s *Scheduler) AddJob(ctx context.Context, name string, timeout time.Duration, cron string, weight int64) error {
	return s.svc.Add(ctx, domain.Job{
		Name:     name,
		Timeout:  timeout,
		Cron:     cron,
		Weight:   weight,
		Executor: string(executor.ExecutorTypeLocalFunc),
	})
}

type ScheduleOption struct {
	Idx int
	F   func(*SchedulerConfig)
}

type SchedulerConfig struct {
	DB *gorm.DB
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
	return ScheduleOption{
		Idx: 0,
		F:   func(c *SchedulerConfig) { c.RefreshInterval = interval },
	}
}
func WithTimeoutInterval(interval time.Duration) ScheduleOption {
	return ScheduleOption{
		Idx: 1,
		F:   func(c *SchedulerConfig) { c.TimeoutInterval = interval },
	}

}
func WithMaxConcurrentPreemptNum(num int) ScheduleOption {
	return ScheduleOption{
		Idx: 2,
		F:   func(c *SchedulerConfig) { c.MaxConcurrentPreemptNum = num },
	}
}

func WithLongScheduleStrategy() ScheduleOption {
	return ScheduleOption{
		Idx: 3,
		F: func(c *SchedulerConfig) {
			c.ScheduleSg = NewLongScheduleStrategy()
		},
	}
}

func WithLoadBalancePreemptStrategy(threshold int64) ScheduleOption {
	return ScheduleOption{
		Idx: 4,
		F: func(c *SchedulerConfig) {
			//访问c.DB的前提是c.DB是非空的
			c.PreemptSg = internal.NewLoadBalancerStrategy(c.DB, threshold, c.TimeoutInterval)
		},
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
	PreemptSg  internal.PreemptStrategy
	//负载数值，在负载均衡抢占策略里面会用到，
	//抢到job了就增加job对应的负载，释放的时候减少负载，也可以用户自己设定负载
	Load            int64
	RefreshInterval time.Duration
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
			//限制抢占的任务数，如果数量达到限制值，阻塞在这里，不去抢占
			err := s.limiter.Acquire(ctx, 1)
			if err != nil {
				return err
			}
			job, err := s.svc.Preempt(ctx, s.Name)

			//没抢到就release信号量，抢到了等执行完成再release
			if err != nil {
				s.limiter.Release(1)
			}
			switch err {
			case context.DeadlineExceeded:
				s.l.Warn("抢占操作超时", logx.Error(err))
				continue
			case gorm.ErrRecordNotFound:
				continue
			case internal.NotMinLoadScr:
				continue
			case internal.ErrNoAvailableJob:
				continue
			case internal.ErrNoAvailableScr:
				s.l.Warn("没有可用的SCR")
				continue
			case nil:
			default:
				s.l.Error("抢占出现错误", logx.Error(err))
				return err
			}

			// 接下来就是执行
			// 怎么执行？--异步执行，不要阻塞主调度循环
			go func(j domain.Job) {
				if s.PreemptSg.Name() == string(LoadBalancerPreemptType) {
					s.AddLoad(j.Weight)
					_ = s.svc.SetLoad(ctx, s.Name, s.GetLoad())
				}
				//开启续约，续约失败则任务执行循环退出；任务循环return了 续约也会被终止
				refreshCtx, refreshCancel := context.WithCancel(ctx)
				go func() {
					er := s.Refresh(refreshCtx, j)
					if er != nil {
						refreshCancel()
					}
				}()

				defer func() {
					s.limiter.Release(1)
					//调度完成，终止续约，调2次cancel没事
					refreshCancel()
					if s.PreemptSg.Name() == string(LoadBalancerPreemptType) {
						s.SubLoad(j.Weight)
						_ = s.svc.SetLoad(ctx, s.Name, s.GetLoad())
					}
					releaseCtx, releaseCancel := context.WithTimeout(ctx, time.Second)
					er := s.svc.Release(releaseCtx, j.Id, s.Name)
					releaseCancel()

					if er != nil {
						s.l.Error("释放任务失败", logx.Error(er), logx.Int64("jobID", j.Id), logx.String("scheduler", s.Name))
					}
					s.l.Info("释放了任务", logx.Int64("jobid", j.Id), logx.String("scheduler", s.Name))
				}()

				exec, ok := s.execs[executor.ExecutorType(j.Executor)]
				if !ok {
					s.l.Error("未找到对应的执行器",
						logx.String("executor", j.Executor))
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
					select {
					case <-refreshCtx.Done():
						return
					case <-time.After(duration):
					}
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

func (s *Scheduler) AddLoad(delta int64) {
	atomic.AddInt64(&s.Load, delta)
}

func (s *Scheduler) SubLoad(delta int64) {
	atomic.AddInt64(&s.Load, -delta)
}

func (s *Scheduler) GetLoad() int64 {
	return atomic.LoadInt64(&s.Load)
}

func (s *Scheduler) Refresh(ctx context.Context, j domain.Job) error {
	ticker := time.NewTicker(s.RefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			er := s.svc.Refresh(j.Id)
			if er != nil {
				if errors.Is(er, internal.HasCandidate) {
					s.l.Info("续约终止,有低负载候选了", logx.Int64("id", j.Id), logx.String("scheduler:", s.Name))
				} else {
					s.l.Error("续约失败", logx.Int64("id", j.Id), logx.String("scheduler:", s.Name), logx.Error(er))
				}
				return er
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (s *Scheduler) RefreshSelf(ctx context.Context) error {
	ticker := time.NewTicker(s.RefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			er := s.svc.RefreshScr(s.Name)
			if er != nil {
				s.l.Error("续约失败", logx.String("scheduler:", s.Name), logx.Error(er))
				return er
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func newScheduler(svc internal.JobService, sc SchedulerConfig) *Scheduler {
	return &Scheduler{
		Name:            uuid.New().String(),
		svc:             svc,
		l:               sc.Logger,
		limiter:         semaphore.NewWeighted(int64(sc.MaxConcurrentPreemptNum)),
		ScheduleSg:      sc.ScheduleSg,
		PreemptSg:       sc.PreemptSg,
		execs:           make(map[executor.ExecutorType]executor.Executor),
		RefreshInterval: sc.RefreshInterval,
	}
}
