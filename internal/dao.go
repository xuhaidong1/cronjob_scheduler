package internal

import (
	"context"
	"errors"
	"gorm.io/gorm"
	"log"
	"time"
)

var ReleaseErr = errors.New("释放任务失败")
var ErrNoAvailableScr = errors.New("没有可用的SCR")
var ErrNoAvailableJob = errors.New("没有可用的Job")

type JobDAO interface {
	// Preempt 抢占执行某job（1次）
	Preempt(ctx context.Context, scrName string) (Job, error)
	// Release 释放job
	Release(ctx context.Context, id int64, selfScrName string) error
	// Create 创建一个Job
	Create(ctx context.Context, j Job) error
	// Delete unregister时用
	Delete(ctx context.Context, name string) error
	GetJobInfo(ctx context.Context, id int64) (Job, error)
	UpdateStatus(ctx context.Context, id int64, status int, scrName string) error
	UpdateCandidate(ctx context.Context, id int64, candi string) error
	// UpdateUtime 通过更新Utime来续约
	UpdateUtime(ctx context.Context, id int64) error
	// UpdateNextTime job完成之后更新下次调度时间
	UpdateNextTime(ctx context.Context, id int64, next time.Time) error
	Stop(ctx context.Context, id int64) error
	Start(ctx context.Context, id int64) error
	//scr管理
	RegisterScheduler(ctx context.Context, scrName string) error
	SetLoad(ctx context.Context, scrName string, load int64) error
	UpdateScrUtime(ctx context.Context, name string) error
	SetDowngrade(ctx context.Context, scrName string, dg bool) error
	GetAllScrLoads(ctx context.Context) ([]Scheduler, error)
}

type GORMJobDAO struct {
	db        *gorm.DB
	preemptSg PreemptStrategy
}

func (g *GORMJobDAO) GetJobInfo(ctx context.Context, id int64) (Job, error) {
	var j = Job{Id: id}
	err := g.db.WithContext(ctx).First(&j).Error
	return j, err
}

func (g *GORMJobDAO) UpdateCandidate(ctx context.Context, id int64, candi string) error {
	return g.db.WithContext(ctx).Model(&Job{}).Where("id = ?", id).Updates(map[string]any{
		"candidate": candi,
		"utime":     time.Now().UnixMilli(),
	}).Error
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
		Where("id = ?", id).Updates(map[string]any{
		"utime": time.Now().UnixMilli(),
	}).Error
}

func (g *GORMJobDAO) UpdateStatus(ctx context.Context, id int64, status int, scrName string) error {
	return g.db.WithContext(ctx).Model(&Job{}).
		Where("id = ?", id).Updates(map[string]any{
		"status": status,
		"utime":  time.Now().UnixMilli(),
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

func (g *GORMJobDAO) Release(ctx context.Context, id int64, selfScrName string) error {
	res := g.db.WithContext(ctx).Model(&Job{}).Where("id =? AND scheduler = ?", id, selfScrName).
		Updates(map[string]any{
			"status":    JobStatusWaiting,
			"scheduler": "",
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
	j, err := g.preemptSg.GetJob(ctx, scrName)
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
			"candidate": "",
			"version":   j.Version + 1,
		})
	if res.Error != nil {
		return Job{}, err
	}
	if res.RowsAffected == 0 {
		// 乐观锁抢占失败，要继续下一轮
		return Job{}, ErrNoAvailableJob
	}
	//拿到了任务，数据库里面这个job的version已经被+1,这里对象也要+1
	j.Version = j.Version + 1
	return j, nil
}

func (g *GORMJobDAO) RegisterScheduler(ctx context.Context, scrName string) error {
	now := time.Now().UnixMilli()
	return g.db.WithContext(ctx).Create(&Scheduler{
		Name:  scrName,
		SLoad: 0,
		Ctime: now,
		Utime: now,
	}).Error
}

func (g *GORMJobDAO) UpdateScrUtime(ctx context.Context, name string) error {
	rf := g.db.WithContext(ctx).Model(&Scheduler{}).Where("name = ?", name).Updates(map[string]any{
		"utime": time.Now().UnixMilli(),
	})
	if rf.RowsAffected == 0 {
		log.Println("scr续约异常", name, rf.Error)
		return errors.New("scr续约异常")
	}
	return nil
}

func (g *GORMJobDAO) SetLoad(ctx context.Context, scrName string, load int64) error {
	rf := g.db.WithContext(ctx).Model(&Scheduler{}).Where("name = ?", scrName).Updates(map[string]any{
		"s_load": load,
		"utime":  time.Now().UnixMilli(),
	})
	if rf.RowsAffected == 0 {
		log.Println("设置负载异常", scrName, rf.Error)
		return errors.New("设置负载异常")
	}
	return nil
}

func (g *GORMJobDAO) GetAllScrLoads(ctx context.Context) ([]Scheduler, error) {
	var scrs []Scheduler
	err := g.db.WithContext(ctx).Model(&Scheduler{}).Where("downgrade = ?", false).Find(&scrs).Error
	return scrs, err
}

func (g *GORMJobDAO) SetDowngrade(ctx context.Context, scrName string, dg bool) error {
	rf := g.db.WithContext(ctx).Model(&Scheduler{}).Where("name = ?", scrName).Updates(map[string]any{
		"downgrade": dg,
		"utime":     time.Now().UnixMilli(),
	})
	if rf.RowsAffected == 0 {
		log.Println("设置降级异常", scrName, rf.Error)
		return errors.New("设置降级异常")
	}
	return nil
}

type Job struct {
	Id       int64  `gorm:"primaryKey,autoIncrement"`
	Name     string `gorm:"unique"`
	Config   string
	Executor string `gorm:"type=varchar(64);not null;"`
	Weight   int64  `gorm:"not null;default:0"`
	//任务执行的超时时间
	Timeout time.Duration
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
	//乐观锁版本
	Version   int64
	Scheduler string `gorm:"type=varchar(128);not null;"`
	Candidate string `gorm:"type=varchar(128);not null;"`
	// 创建时间，毫秒数
	Ctime int64
	// 更新时间，毫秒数
	Utime int64
}

type Scheduler struct {
	Id        int64  `gorm:"primaryKey,autoIncrement"`
	Name      string `gorm:"type=varchar(128);not null;unique"`
	SLoad     int64  `gorm:"not null;default:0;index:utime_load,1"`
	Downgrade bool   `gorm:"not null;default:false"`
	// 创建时间，毫秒数
	Ctime int64
	// 更新时间，毫秒数
	Utime int64 `gorm:"not null;default:0;index:utime_load,2"`
}

const (
	JobStatusWaiting = iota
	// JobStatusRunning 已经被抢占
	JobStatusRunning
	// JobStatusOccupied 被长抢占所占有
	JobStatusOccupied
	// JobStatusToHandOver 调度者负载过高，希望交接job给指定候选者
	JobStatusToHandOver
	// JobStatusPaused 暂停调度
	JobStatusPaused
)

func InitTable(db *gorm.DB) error {
	return db.AutoMigrate(&Job{}, &Scheduler{})
}
