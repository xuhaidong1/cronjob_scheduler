package cronjob_scheduler

type ScheduleStrategyType string

const (
	LongScheduleType ScheduleStrategyType = "long"
	OnceScheduleType ScheduleStrategyType = "once"
)

const (
	TimeoutPreemptType      string = "timeout"
	LoadBalancerPreemptType string = "loadbalancer"
)
