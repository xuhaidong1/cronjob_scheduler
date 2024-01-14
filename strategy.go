package cronjob_scheduler

import "github.com/xuhaidong1/cronjob_scheduler/internal"

type ScheduleStrategyType string

const (
	LongScheduleType ScheduleStrategyType = "long"
	OnceScheduleType ScheduleStrategyType = "once"
)

const (
	TimeoutPreemptType      string = "timeout"
	LoadBalancerPreemptType string = "loadbalancer"
)

type ReBalanceStrategy string

const (
	RelaxReBalanceStrategy  ReBalanceStrategy = ReBalanceStrategy(internal.RelaxReBalance)
	StrictReBalanceStrategy                   = ReBalanceStrategy(internal.StrictReBalance)
)
