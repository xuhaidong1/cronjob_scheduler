package internal

import (
	"context"
	domain "github.com/xuhaidong1/cronjob_scheduler/domain"
	"time"
)

type JobRepository interface {
	Preempt(ctx context.Context, scrName string) (domain.Job, error)
	Release(ctx context.Context, j domain.Job) error
	Create(ctx context.Context, j domain.Job) error
	Delete(ctx context.Context, name string) error
	UpdateUtime(ctx context.Context, id int64) error
	UpdateNextTime(ctx context.Context, id int64, next time.Time) error
	Stop(ctx context.Context, id int64) error
	Start(ctx context.Context, id int64) error
}

type CronJobRepository struct {
	dao JobDAO
}

func NewCronJobRepository(dao JobDAO) *CronJobRepository {
	return &CronJobRepository{dao: dao}
}

func (p *CronJobRepository) Create(ctx context.Context, j domain.Job) error {
	return p.dao.Create(ctx, Job{
		Config:   j.Config,
		Executor: j.Executor,
		Name:     j.Name,
		NextTime: j.NextTime().UnixMilli(),
		Status:   JobStatusWaiting,
		Cron:     j.Cron,
		Version:  0,
	})
}

func (p *CronJobRepository) Delete(ctx context.Context, name string) error {
	return p.dao.Delete(ctx, name)
}

func (p *CronJobRepository) UpdateUtime(ctx context.Context, id int64) error {
	return p.dao.UpdateUtime(ctx, id)
}

func (p *CronJobRepository) UpdateNextTime(ctx context.Context, id int64, next time.Time) error {
	return p.dao.UpdateNextTime(ctx, id, next)
}

func (p *CronJobRepository) Release(ctx context.Context, j domain.Job) error {
	return p.dao.Release(ctx, Job{
		Id:      j.Id,
		Version: j.Version,
	})
}

func (p *CronJobRepository) Preempt(ctx context.Context, scrName string) (domain.Job, error) {
	j, err := p.dao.Preempt(ctx, scrName)
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
func (p *CronJobRepository) Stop(ctx context.Context, id int64) error {
	return p.dao.Stop(ctx, id)
}
func (p *CronJobRepository) Start(ctx context.Context, id int64) error {
	return p.dao.Start(ctx, id)
}
