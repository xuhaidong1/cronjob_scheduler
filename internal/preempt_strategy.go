package internal

import (
	"context"
	"errors"
	"gorm.io/gorm"
	"log"
	"time"
)

var NotMinLoadScr = errors.New("不是最小负载的scr")

type PreemptStrategy interface {
	Name() string
	GetJob(ctx context.Context, selfScrName string) (Job, error)
}

// TimeoutPreemptStrategy 首先考虑接手续约失败的任务，然后考虑就绪任务
type TimeoutPreemptStrategy struct {
	// 如果一个running的job，超过timeout时间没有被续约，
	// 我们认为这个job续约失败。可以剥夺这个job
	timeout time.Duration
	db      *gorm.DB
}

func (p *TimeoutPreemptStrategy) Name() string {
	return "timeout"
}

func (p *TimeoutPreemptStrategy) GetJob(ctx context.Context, selfScrName string) (Job, error) {
	now := time.Now()
	//先看看有没有续约失败的任务
	var j Job
	db := p.db.WithContext(ctx)
	err := db.Where("status = ? AND utime <=?", JobStatusRunning, now.Add(-p.timeout).UnixMilli()).
		First(&j).Error
	if err == nil {
		return j, nil
	}
	//没有的话查找可执行的任务
	err = db.Where("status = ? AND next_time <=?", JobStatusWaiting, now.UnixMilli()).
		First(&j).Error
	return j, err
}

func NewTimeoutPreemptStrategy(db *gorm.DB, timeout time.Duration) PreemptStrategy {
	return &TimeoutPreemptStrategy{db: db, timeout: timeout}
}

// LoadBalancerStrategy 负载均衡策略，首先考虑剥夺高负载节点正在执行的任务，然后考虑续约失败的任务，最后考虑就绪任务
type LoadBalancerStrategy struct {
	//这个阈值代表了发生负载均衡的阈值，若对于某个被抢占的job，如果它的scheduler的负载比它的候选者（若有）高出threshold，
	//那么该scheduler在续约时就把任务让出去，由候选者抢占执行
	//当然一个job的最大weight不能超过threshold，若超过，就会出现踢皮球的场景
	threshold int64
	// 如果一个running的job，超过timeout时间没有被续约，
	// 我们认为这个job续约失败。可以剥夺这个job
	timeout time.Duration
	db      *gorm.DB
}

func (p *LoadBalancerStrategy) GetJob(ctx context.Context, selfScrName string) (Job, error) {
	now := time.Now()
	db := p.db.WithContext(ctx)
	var j Job
	//先看看候选人是自己的job有没有被释放，如果被释放了，就抢占//release时记得删除候选者
	//err := db.Where("candidate = ? AND status = ? AND next_time <= ?", selfScrName, JobStatusWaiting, now.UnixMilli()).
	//	Order("weight ASC").Order("timeout ASC").First(&j).Error
	//if err == nil {
	//	log.Println(selfScrName, "抢到了高负载scr的job", j)
	//	return j, err
	//}

	//再看有没有续约失败的任务
	err := db.Where("status = ? AND utime <=?", JobStatusRunning, now.Add(-p.timeout).UnixMilli()).
		First(&j).Error
	if err == nil {
		log.Println(selfScrName, "抢到了续约失败scr的job", j.Name)
		return j, nil
	}
	//没有的话查找被负载均衡分配过的任务
	//err = db.Where("status = ? AND next_time <=? AND candidate_history = ? AND candidate = ?", JobStatusWaiting, now.UnixMilli(), selfScrName, "").
	//	First(&j).Error
	//if err == nil {
	//	log.Println(selfScrName, "抢到了历史分配过的job", j.Name)
	//	return j, nil
	//}
	//没有的话查找可执行的任务
	var s Scheduler
	err = db.Order("s_load ASC").Where("utime >= ?", time.Now().Add(-p.timeout).UnixMilli()).First(&s).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return Job{}, err
	}
	err = db.Where("status = ? AND next_time <=? AND candidate = ?", JobStatusWaiting, now.UnixMilli(), "").
		First(&j).Error
	if err == nil {
		if s.Name != selfScrName {
			return Job{}, NotMinLoadScr
		}
		log.Println(selfScrName, "抢到了正常的job", j.Name)
	}
	return j, err
}

func NewLoadBalancerStrategy(db *gorm.DB, threshold int64, timeout time.Duration) PreemptStrategy {
	return &LoadBalancerStrategy{db: db, threshold: threshold, timeout: timeout}
}

func (p *LoadBalancerStrategy) Name() string {
	return "loadbalancer"
}
