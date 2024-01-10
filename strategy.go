package cronjob_scheduler

type ScheduleStrategyType string

const (
	LongScheduleType ScheduleStrategyType = "long"
	OnceScheduleType ScheduleStrategyType = "once"
)

type PreemptStrategyType string

const (
	TimeoutPreemptType      PreemptStrategyType = "timeout"
	LoadBalancerPreemptType PreemptStrategyType = "loadbalancer"
)
