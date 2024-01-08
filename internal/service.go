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
	Release(ctx context.Context, j domain.Job) error
	Register(ctx context.Context, j domain.Job) error
	UnRegister(ctx context.Context, name string) error
	Stop(ctx context.Context, id int64) error
}

type cronJobService struct {
	repo            JobRepository
	refreshInterval time.Duration
	l               logx.Logger
}

func (s *cronJobService) Register(ctx context.Context, j domain.Job) error {
	return s.repo.Create(ctx, j)
}

func (s *cronJobService) UnRegister(ctx context.Context, name string) error {
	return s.repo.Delete(ctx, name)
}

func (s *cronJobService) Release(ctx context.Context, j domain.Job) error {
	return s.repo.Release(ctx, j)
}

func (s *cronJobService) Stop(ctx context.Context, id int64) error {
	return s.repo.Stop(ctx, id)
}

func (s *cronJobService) Preempt(ctx context.Context, scrName string) (domain.Job, error) {
	j, err := s.repo.Preempt(ctx, scrName)
	if err != nil {
		return domain.Job{}, err
	}
	ticker := time.NewTicker(s.refreshInterval)
	// 抢占之后，一直抢占着吗？
	// 考虑一个释放的问题:续约失败释放 & 任务完成释放
	refreshCtx, refreshCancel := context.WithCancel(ctx)
	j.CancelFunc = func() error {
		ticker.Stop()
		refreshCancel()
		ct, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return s.repo.Release(ct, j)
	}
	//续约
	go func() {
		for {
			select {
			case <-ticker.C:
				er := s.refresh(j.Id)
				if er != nil {
					s.l.Error("续约失败", logx.Int64("id", j.Id), logx.Error(er))
					e := j.CancelFunc()
					if e != nil {
						s.l.Error("续约释放失败", logx.Int64("id", j.Id), logx.Error(e))
					}
					return
				}
			case <-refreshCtx.Done():
				return
			}
		}
	}()

	return j, err
}

func (s *cronJobService) SetNextTime(ctx context.Context, j domain.Job) error {
	next := j.NextTime()
	if next.IsZero() {
		// 没有下一次
		return s.repo.Stop(ctx, j.Id)
	}
	return s.repo.UpdateNextTime(ctx, j.Id, next)
}

func (s *cronJobService) refresh(id int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// 续约怎么个续法？
	// 更新一下更新时间就可以
	// 比如说我们的续约失败逻辑就是：处于 running 状态，但是更新时间在一分钟以前
	return s.repo.UpdateUtime(ctx, id)
}

func NewCronJobService(repo JobRepository, refreshInterval time.Duration, l logx.Logger) JobService {
	return &cronJobService{repo: repo, refreshInterval: refreshInterval, l: l}
}
