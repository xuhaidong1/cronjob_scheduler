package strategy

import (
	"context"
	"github.com/xuhaidong1/cronjob_scheduler/internal"
)

type PreemptStrategy interface {
	GetJob(ctx context.Context) (internal.Job, error)
}

// TimeoutPreemptStrategy 首先考虑接手续约失败的任务，然后考虑就绪任务
type TimeoutPreemptStrategy struct {
}

// LoadBalancerStrategy 负载均衡策略，首先考虑剥夺高负载节点正在执行的任务，然后考虑续约失败的任务，最后考虑就绪任务
type LoadBalancerStrategy struct {
}
