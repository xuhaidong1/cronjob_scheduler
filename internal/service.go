package internal

import (
	"context"
	"errors"
	"github.com/xuhaidong1/cronjob_scheduler/domain"
	"github.com/xuhaidong1/go-generic-tools/pluginsx/logx"
	"log"
	"sort"
	"time"
)

var ErrSelfLoadTooHigh = errors.New("自身负载过高，中断续约job")

type JobService interface {
	Preempt(ctx context.Context, scrName string) (domain.Job, error)
	SetNextTime(ctx context.Context, j domain.Job) error
	// Refresh job续约
	Refresh(j domain.Job, scrLoad ...int64) error
	RefreshScr(name string) error
	Release(ctx context.Context, id int64, selfScrName string) error
	Add(ctx context.Context, j domain.Job) error
	Delete(ctx context.Context, name string) error
	Stop(ctx context.Context, id int64) error
	RegisterScheduler(ctx context.Context, scrName string) error
	ReBalanceFailedJob() <-chan domain.Job
	SetLoad(ctx context.Context, scrName string, load int64) error
	SetDowngrade(ctx context.Context, scrName string, dg bool) error
	UpdateStatus(ctx context.Context, id int64, status int, scrName string) error
}

type cronJobService struct {
	repo               JobRepository
	refreshInterval    time.Duration
	l                  logx.Logger
	rb                 ReBalanceStrategy
	reBalanceFailedJob chan domain.Job
	IsOutlier          IsOutlier
}

func (s *cronJobService) UpdateStatus(ctx context.Context, id int64, status int, scrName string) error {
	return s.repo.UpdateStatus(ctx, id, status, scrName)
}

func (s *cronJobService) RefreshScr(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// 续约怎么个续法？
	// 更新一下更新时间就可以
	// 比如说我们的判断离线逻辑就是：更新时间在一分钟以前
	return s.repo.UpdateScrUtime(ctx, name)
}

func (s *cronJobService) Add(ctx context.Context, j domain.Job) error {
	return s.repo.Create(ctx, j)
}

func (s *cronJobService) Delete(ctx context.Context, name string) error {
	return s.repo.Delete(ctx, name)
}

func (p *cronJobService) RegisterScheduler(ctx context.Context, scrName string) error {
	return p.repo.RegisterScheduler(ctx, scrName)
}

func (p *cronJobService) SetLoad(ctx context.Context, scrName string, load int64) error {
	return p.repo.SetLoad(ctx, scrName, load)
}

func (s *cronJobService) Release(ctx context.Context, id int64, selfScrName string) error {
	return s.repo.Release(ctx, id, selfScrName)
}

func (s *cronJobService) Stop(ctx context.Context, id int64) error {
	return s.repo.Stop(ctx, id)
}

func (s *cronJobService) Preempt(ctx context.Context, scrName string) (domain.Job, error) {
	return s.repo.Preempt(ctx, scrName)
}

func (s *cronJobService) SetNextTime(ctx context.Context, j domain.Job) error {
	next := j.NextTime()
	if next.IsZero() {
		// 没有下一次
		return s.repo.Stop(ctx, j.Id)
	}
	return s.repo.UpdateNextTime(ctx, j.Id, next)
}

func (s *cronJobService) Refresh(j domain.Job, scrLoad ...int64) error {
	if len(scrLoad) != 0 {
		//先查看自己是否负载过高，过高的话可能会释放这个job
		scrs, err := s.repo.GetAllScrLoads(context.Background())
		if err != nil {
			return err
		}
		loads := make([]int64, 0, len(scrs))
		for _, l := range scrs {
			loads = append(loads, l.SLoad)
		}
		if s.IsOutlier.IsOutlier(scrLoad[0], loads) {
			switch s.rb {
			case RelaxReBalance:
				return ErrSelfLoadTooHigh
			case StrictReBalance:
				candi := ""
				sort.Slice(scrs, func(i, j int) bool {
					return scrs[i].SLoad < scrs[j].SLoad
				})
				log.Println(scrs)
				for _, scr := range scrs {
					//候选节点负载低，健康 且 没降级
					if !scr.Downgrade {
						candi = scr.Name
						break
					}
				}
				if candi != "" {
					_ = s.repo.UpdateCandidate(context.Background(), j.Id, candi)
					now := time.Now()
					//看一下过了1个周期之后如果候选没有解盘，就自己拿回这个job
					go func() {
						time.Sleep(j.NextTime().Sub(time.Now()))
						time.Sleep(time.Minute)
						job, _ := s.repo.GetJobInfo(context.Background(), j.Id)
						if job.Utime <= now.UnixMilli() {
							//没有人解盘，抢回job
							s.reBalanceFailedJob <- j
						}
					}()
					return ErrSelfLoadTooHigh
				}
				//如果没有合适节点接盘，只能乖乖续约
			}
		}
	}
	// 续约怎么个续法？
	// 更新一下更新时间就可以
	// 比如说我们的续约失败逻辑就是：处于 running 状态，但是更新时间在一分钟以前
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return s.repo.UpdateUtime(ctx, j.Id)
}

func (s *cronJobService) ReBalanceFailedJob() <-chan domain.Job {
	return s.reBalanceFailedJob
}

func (p *cronJobService) SetDowngrade(ctx context.Context, scrName string, dg bool) error {
	return p.repo.SetDowngrade(ctx, scrName, dg)
}

func NewCronJobService(repo JobRepository, l logx.Logger, sg ReBalanceStrategy, IsOutlier IsOutlier) JobService {
	return &cronJobService{repo: repo, l: l, reBalanceFailedJob: make(chan domain.Job), rb: sg, IsOutlier: IsOutlier}
}
