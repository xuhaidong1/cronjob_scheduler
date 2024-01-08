package internal

import (
	"context"
	"errors"
	"gorm.io/gorm"
	"time"
)

var ReleaseErr = errors.New("释放任务失败")

type JobDAO interface {
	// Preempt 抢占执行某job（1次）
	Preempt(ctx context.Context, scrName string) (Job, error)
	// Release 释放job
	Release(ctx context.Context, j Job) error
	// Create 创建一个Job
	Create(ctx context.Context, j Job) error
	// Delete unregister时用
	Delete(ctx context.Context, name string) error
	// UpdateUtime 通过更新Utime来续约
	UpdateUtime(ctx context.Context, id int64) error
	// UpdateNextTime job完成之后更新下次调度时间
	UpdateNextTime(ctx context.Context, id int64, next time.Time) error
	Stop(ctx context.Context, id int64) error
	Start(ctx context.Context, id int64) error
}

type GORMJobDAO struct {
	db        *gorm.DB
	preemptSg PreemptStrategy
}

func NewGORMJobDAO(db *gorm.DB, preemptSg PreemptStrategy) JobDAO {
	return &GORMJobDAO{db: db, preemptSg: preemptSg}
}

func (g *GORMJobDAO) Create(ctx context.Context, j Job) error {
	now := time.Now().UnixMilli()
	j.Utime = now
	j.Ctime = now
	return g.db.WithContext(ctx).Create(&j).Error
}

func (g *GORMJobDAO) Delete(ctx context.Context, name string) error {
	j := &Job{Name: name}
	return g.db.WithContext(ctx).Delete(&j).Where("name = ?", j.Name).Error
}

func (g *GORMJobDAO) UpdateUtime(ctx context.Context, id int64) error {
	return g.db.WithContext(ctx).Model(&Job{}).
		Where("id =?", id).Updates(map[string]any{
		"utime": time.Now().UnixMilli(),
	}).Error
}

func (g *GORMJobDAO) UpdateNextTime(ctx context.Context, id int64, next time.Time) error {
	return g.db.WithContext(ctx).Model(&Job{}).
		Where("id = ?", id).Updates(map[string]any{
		"next_time": next.UnixMilli(),
		"utime":     time.Now().UnixMilli(),
	}).Error
}

func (g *GORMJobDAO) Stop(ctx context.Context, id int64) error {
	return g.db.WithContext(ctx).
		Where("id = ?", id).Updates(map[string]any{
		"status": JobStatusPaused,
		"utime":  time.Now().UnixMilli(),
	}).Error
}

func (g *GORMJobDAO) Start(ctx context.Context, id int64) error {
	return g.db.WithContext(ctx).
		Where("id = ?", id).Updates(map[string]any{
		"status": JobStatusWaiting,
		"utime":  time.Now().UnixMilli(),
	}).Error
}

func (g *GORMJobDAO) Release(ctx context.Context, j Job) error {
	// id=? and status =running and scheduler = me
	res := g.db.WithContext(ctx).Model(&Job{}).Where("id =? AND status = ?", j.Id, JobStatusRunning).
		Updates(map[string]any{
			"status":    JobStatusWaiting,
			"scheduler": "",
			"utime":     time.Now().UnixMilli(),
		})
	if res.RowsAffected == 0 {
		return ReleaseErr
	}
	return res.Error
}

func (g *GORMJobDAO) Preempt(ctx context.Context, scrName string) (Job, error) {
	// 高并发情况下，会有性能问题
	// 100 个 goroutine
	// 要转几次？ 所有 goroutine 执行的循环次数加在一起是
	// 1+2+3+4 +5 + ... + 99 + 100
	// 特定一个 goroutine，最差情况下，要循环一百次
	db := g.db.WithContext(ctx)
	j, err := g.preemptSg.GetJob(ctx)
	if err != nil {
		return Job{}, err
	}
	// 两个 goroutine 都拿到 id =1 的数据
	// 乐观锁，CAS
	res := db.Where("id=? AND version = ?", j.Id, j.Version).Model(&Job{}).
		Updates(map[string]any{
			"status":    JobStatusRunning,
			"utime":     time.Now().UnixMilli(),
			"scheduler": scrName,
			"version":   j.Version + 1,
		})
	if res.Error != nil {
		return Job{}, err
	}
	if res.RowsAffected == 0 {
		// 乐观锁抢占失败，要继续下一轮
		return Job{}, err
	}
	//拿到了任务，数据库里面这个job的version已经被+1,这里对象也要+1
	j.Version = j.Version + 1
	return j, nil
}

type Job struct {
	Id        int64 `gorm:"primaryKey,autoIncrement"`
	Config    string
	Executor  string `gorm:"type=varchar(64);not null;"`
	Scheduler string `gorm:"type=varchar(128);not null;"`
	Candidate string `gorm:"type=varchar(128);not null;"`
	Name      string `gorm:"unique"`

	// 定时任务，我怎么知道，已经到时间了呢？
	// NextTime 下一次被调度的时间
	// next_time <= now 这样一个查询条件
	// and status = 0
	// 建立 next_time 和 status 的联合索引
	NextTime int64 `gorm:"index:nexttime_status"`

	// 用状态来标记
	Status int `gorm:"index:nexttime_status"`

	// cron 表达式
	Cron string

	Version int64

	// 创建时间，毫秒数
	Ctime int64
	// 更新时间，毫秒数
	Utime int64
}

const (
	JobStatusWaiting = iota
	// JobStatusRunning 已经被抢占
	JobStatusRunning
	// JobStatusPaused 暂停调度
	JobStatusPaused
)

func InitTable(db *gorm.DB) error {
	return db.AutoMigrate(&Job{})
}
