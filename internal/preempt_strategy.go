package internal

import (
	"context"
	"gorm.io/gorm"
	"time"
)

type PreemptStrategy interface {
	GetJob(ctx context.Context) (Job, error)
}

// TimeoutPreemptStrategy 首先考虑接手续约失败的任务，然后考虑就绪任务
type TimeoutPreemptStrategy struct {
	// 如果一个running的job，超过timeout时间没有被续约，
	// 我们认为这个job续约失败。可以剥夺这个job
	timeout time.Duration
	db      *gorm.DB
}

func (p *TimeoutPreemptStrategy) GetJob(ctx context.Context) (Job, error) {
	now := time.Now()
	//先看看有没有续约失败的任务
	var j Job
	db := p.db.WithContext(ctx)
	err := db.Where("status = ? AND utime <=?", JobStatusRunning, now.Add(-p.timeout).UnixMilli()).
		First(&j).Error
	if err != nil {
		//没有的话查找可执行的任务
		err = db.Where("status = ? AND next_time <=?", JobStatusWaiting, now.UnixMilli()).
			First(&j).Error
		if err != nil {
			//没有任务/出错，从这里返回
			return Job{}, err
		}
	}
	return j, err
}

func NewTimeoutPreemptStrategy(db *gorm.DB, timeout time.Duration) PreemptStrategy {
	return &TimeoutPreemptStrategy{db: db, timeout: timeout}
}

// LoadBalancerStrategy 负载均衡策略，首先考虑剥夺高负载节点正在执行的任务，然后考虑续约失败的任务，最后考虑就绪任务
type LoadBalancerStrategy struct {
}

func (l LoadBalancerStrategy) GetJob(ctx context.Context) (Job, error) {
	//TODO implement me
	panic("implement me")
}

func NewLoadBalancerStrategy() PreemptStrategy {
	return &LoadBalancerStrategy{}
}

type PreemptStrategyType string
