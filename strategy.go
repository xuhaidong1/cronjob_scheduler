package cronjob_scheduler

type ScheduleStrategyType string

const (
	LongPreemptStrategyType ScheduleStrategyType = "long"
	OncePreemptStrategyType ScheduleStrategyType = "once"
)

type PreemptStrategyType string

const (
	TimeoutPreemptStrategyType PreemptStrategyType = "timeout"
	LoadBalancerStrategyType   PreemptStrategyType = "loadbalancer"
)
