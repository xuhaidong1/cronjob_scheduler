package internal

import (
	"context"
	"github.com/xuhaidong1/cronjob_scheduler/domain"
	"github.com/xuhaidong1/go-generic-tools/pluginsx/logx"
	"time"
)

type JobService interface {
	Preempt(ctx context.Context, scrName string) (domain.Job, error)
	SetNextTime(ctx context.Context, j domain.Job) error
	Refresh(id int64) error
	RefreshScr(name string) error
	Release(ctx context.Context, id int64, selfScrName string) error
	Add(ctx context.Context, j domain.Job) error
	Delete(ctx context.Context, name string) error
	Stop(ctx context.Context, id int64) error
	RegisterScheduler(ctx context.Context, scrName string) error
	SetLoad(ctx context.Context, scrName string, load int64) error
}

type cronJobService struct {
	repo            JobRepository
	refreshInterval time.Duration
	l               logx.Logger
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

func (s *cronJobService) Refresh(id int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// 续约怎么个续法？
	// 更新一下更新时间就可以
	// 比如说我们的续约失败逻辑就是：处于 running 状态，但是更新时间在一分钟以前
	return s.repo.UpdateUtime(ctx, id)
}

func NewCronJobService(repo JobRepository, l logx.Logger) JobService {
	return &cronJobService{repo: repo, l: l}
}
