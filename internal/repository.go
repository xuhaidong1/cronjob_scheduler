package internal

import (
	"context"
	domain "github.com/xuhaidong1/cronjob_scheduler/domain"
	"time"
)

type JobRepository interface {
	Preempt(ctx context.Context) (domain.Job, error)
	Release(ctx context.Context, j domain.Job) error
	UpdateUtime(ctx context.Context, id int64) error
	UpdateNextTime(ctx context.Context, id int64, next time.Time) error
	Stop(ctx context.Context, id int64) error
	Start(ctx context.Context, id int64) error
}

type PreemptCronJobRepository struct {
	dao JobDAO
}

func (p *PreemptCronJobRepository) UpdateUtime(ctx context.Context, id int64) error {
	return p.dao.UpdateUtime(ctx, id)
}

func (p *PreemptCronJobRepository) UpdateNextTime(ctx context.Context, id int64, next time.Time) error {
	return p.dao.UpdateNextTime(ctx, id, next)
}

func (p *PreemptCronJobRepository) Release(ctx context.Context, j domain.Job) error {
	return p.dao.Release(ctx, Job{
		Id:      j.Id,
		Version: j.Version,
	})
}

func (p *PreemptCronJobRepository) Preempt(ctx context.Context) (domain.Job, error) {
	j, err := p.dao.Preempt(ctx)
	if err != nil {
		return domain.Job{}, err
	}
	return domain.Job{
		Config:   j.Config,
		Id:       j.Id,
		Name:     j.Name,
		Cron:     j.Cron,
		Executor: j.Executor,
	}, nil
}
func (p *PreemptCronJobRepository) Stop(ctx context.Context, id int64) error {
	return p.dao.Stop(ctx, id)
}
func (p *PreemptCronJobRepository) Start(ctx context.Context, id int64) error {
	return p.dao.Start(ctx, id)
}
