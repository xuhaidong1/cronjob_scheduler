package internal

import (
	"context"
	"errors"
	"gorm.io/gorm"
	"log"
	"time"
)

var ReleaseErr = errors.New("释放任务失败")
var HasCandidate = errors.New("有候选了，需要让出job")
var RefreshFailed = errors.New("续约失败，dao")
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
		Where("id = ?", id).Updates(map[string]any{
		"utime": time.Now().UnixMilli(),
	}).Error
}

//func (g *GORMJobDAO) UpdateSchedulerLoad(ctx context.Context, id, load int64, scrName string) error {
//	var count int64
//	err := g.db.WithContext(ctx).Model(&Job{}).
//		Where("id = ? AND scheduler = ? AND candidate <> ? ", id, scrName, "").Count(&count).Error
//	if err != nil {
//		return err
//	}
//	if count != 0 {
//		return HasCandidate
//	}
//	res := g.db.WithContext(ctx).Model(&Job{}).
//		Where("id = ? AND scheduler = ?", id, scrName).Updates(map[string]any{
//		"scheduler_load": load,
//		"utime":          time.Now().UnixMilli(),
//	})
//	if res.Error != nil {
//		return res.Error
//	}
//	if res.RowsAffected == 0 {
//		return RefreshFailed
//	}
//	log.Println("续约成功", scrName, id)
//	return nil
//}

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
	res := g.db.WithContext(ctx).Model(&Job{}).Where("id =? AND status = ? AND scheduler = ?", id, JobStatusRunning, selfScrName).
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
	//defer func() {
	//	//看有没有高负载的scheduler，有的话就把自己加入job候选，下一轮就可能抢占到任务了
	//	//如果此轮抢到了job，还是用0负载来判断就不科学了，所以加上抢占的结果job的权重，如果没有抢到，加的weight是0值
	//	rowsAffected := db.Where("scheduler_load > ? AND status = ? AND candidate = ? AND scheduler <> ?", selfLoad+p.threshold+j.Weight, JobStatusRunning, "", selfScrName).
	//		Order("scheduler_load DESC").First(&j).
	//		Updates(map[string]any{
	//			"candidate": scrName,
	//			//发生了新的负载均衡，历史就不具备参考意义了；
	//			"candidate_history": "",
	//		}).RowsAffected
	//	if rowsAffected != 0 {
	//		log.Println(scrName, "加入了候选", j.Name)
	//	}
	//}()
	if res.Error != nil {
		return Job{}, err
	}
	if res.RowsAffected == 0 {
		// 乐观锁抢占失败，要继续下一轮
		return Job{}, ErrNoAvailableJob
	}
	//拿到了任务，数据库里面这个job的version已经被+1,这里对象也要+1
	j.Version = j.Version + 1
	//如果是从高负载任务那里抢到的，那么记录到负载均衡历史里面，下次优先从历史抢占任务，防止重复负载均衡
	//if j.Candidate == scrName {
	//	er := db.Where("id=? AND version = ?", j.Id, j.Version).Model(&Job{}).
	//		Updates(map[string]any{"candidate_history": scrName}).Error
	//	if er != nil {
	//		log.Println("更新历史候选失败", j)
	//	}
	//}
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
	Version int64
	//负载均衡用到的3个字段
	Scheduler string `gorm:"type=varchar(128);not null;"`
	Candidate string `gorm:"type=varchar(128);not null;"`
	//历史候选，当抢占任务时，优先抢占被负载均衡算法分配到过的任务，防止再次发生不必要的负载均衡，因为定时任务变化不是那么频繁
	CandidateHistory string `gorm:"type=varchar(128);not null;"`
	// 创建时间，毫秒数
	Ctime int64
	// 更新时间，毫秒数
	Utime int64
}

type Scheduler struct {
	Id    int64  `gorm:"primaryKey,autoIncrement"`
	Name  string `gorm:"type=varchar(128);not null;unique"`
	SLoad int64  `gorm:"not null;default:0;index:utime_load,1"`
	// 创建时间，毫秒数
	Ctime int64
	// 更新时间，毫秒数
	Utime int64 `gorm:"not null;default:0;index:utime_load,2"`
}

const (
	JobStatusWaiting = iota
	// JobStatusRunning 已经被抢占
	JobStatusRunning
	// JobStatusPaused 暂停调度
	JobStatusPaused
)

func InitTable(db *gorm.DB) error {
	return db.AutoMigrate(&Job{}, &Scheduler{})
}
