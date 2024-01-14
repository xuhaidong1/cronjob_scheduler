package internal

import (
	"context"
	domain "github.com/xuhaidong1/cronjob_scheduler/domain"
	"time"
)

type JobRepository interface {
	Preempt(ctx context.Context, scrName string) (domain.Job, error)
	Release(ctx context.Context, id int64, selfScrName string) error
	Create(ctx context.Context, j domain.Job) error
	Delete(ctx context.Context, name string) error
	UpdateStatus(ctx context.Context, id int64, status int, scrName string) error
	UpdateCandidate(ctx context.Context, id int64, candi string) error
	UpdateUtime(ctx context.Context, id int64) error
	UpdateScrUtime(ctx context.Context, name string) error
	UpdateNextTime(ctx context.Context, id int64, next time.Time) error
	Stop(ctx context.Context, id int64) error
	Start(ctx context.Context, id int64) error
	RegisterScheduler(ctx context.Context, scrName string) error
	SetLoad(ctx context.Context, scrName string, load int64) error
	SetDowngrade(ctx context.Context, scrName string, dg bool) error
	GetAllScrLoads(ctx context.Context) ([]Scheduler, error)
	GetJobInfo(ctx context.Context, id int64) (Job, error)
}

type CronJobRepository struct {
	dao JobDAO
}

func (p *CronJobRepository) GetJobInfo(ctx context.Context, id int64) (Job, error) {
	return p.dao.GetJobInfo(ctx, id)
}

func (p *CronJobRepository) UpdateCandidate(ctx context.Context, id int64, candi string) error {
	return p.dao.UpdateCandidate(ctx, id, candi)
}

func (p *CronJobRepository) UpdateStatus(ctx context.Context, id int64, status int, scrName string) error {
	return p.dao.UpdateStatus(ctx, id, status, scrName)
}

func (p *CronJobRepository) SetDowngrade(ctx context.Context, scrName string, dg bool) error {
	return p.dao.SetDowngrade(ctx, scrName, dg)
}

func (p *CronJobRepository) UpdateScrUtime(ctx context.Context, name string) error {
	return p.dao.UpdateScrUtime(ctx, name)
}

func (p *CronJobRepository) RegisterScheduler(ctx context.Context, scrName string) error {
	return p.dao.RegisterScheduler(ctx, scrName)
}

func (p *CronJobRepository) SetLoad(ctx context.Context, scrName string, load int64) error {
	return p.dao.SetLoad(ctx, scrName, load)
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
		Timeout:  j.Timeout,
		Status:   JobStatusWaiting,
		Cron:     j.Cron,
		Weight:   j.Weight,
		Version:  0,
	})
}

func (p *CronJobRepository) Delete(ctx context.Context, name string) error {
	return p.dao.Delete(ctx, name)
}

func (p *CronJobRepository) UpdateUtime(ctx context.Context, id int64) error {
	return p.dao.UpdateUtime(ctx, id)
}

//func (p *CronJobRepository) UpdateSchedulerLoad(ctx context.Context, id, load int64, scrName string) error {
//	return p.dao.UpdateSchedulerLoad(ctx, id, load, scrName)
//}

func (p *CronJobRepository) UpdateNextTime(ctx context.Context, id int64, next time.Time) error {
	return p.dao.UpdateNextTime(ctx, id, next)
}

func (p *CronJobRepository) Release(ctx context.Context, id int64, selfScrName string) error {
	return p.dao.Release(ctx, id, selfScrName)
}

func (g *CronJobRepository) GetAllScrLoads(ctx context.Context) ([]Scheduler, error) {
	return g.dao.GetAllScrLoads(ctx)
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
		Timeout:  j.Timeout,
		Weight:   j.Weight,
	}, nil
}
func (p *CronJobRepository) Stop(ctx context.Context, id int64) error {
	return p.dao.Stop(ctx, id)
}
func (p *CronJobRepository) Start(ctx context.Context, id int64) error {
	return p.dao.Start(ctx, id)
}
