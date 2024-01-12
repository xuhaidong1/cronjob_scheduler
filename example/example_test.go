package example

import (
	"context"
	"fmt"
	cs "github.com/xuhaidong1/cronjob_scheduler"
	"github.com/xuhaidong1/cronjob_scheduler/ioc"
	"time"
)

//scheduler提供多种任务定时任务调度策略
//（2）对于任务的调度方式，是抢占式调度（而非分配）；分为了普通抢占策略和负载均衡抢占策略
// 普通抢占策略：对于waiting状态的任务，到了任务的执行时间，就会被某个存活的非降级scr抢占
// 负载均衡抢占策略：对于waiting状态的任务，到了任务的执行时间，会被存活的非降级scr中负载最小的抢占；当scr在续约任务时，若发生自身负载过高，则会释放该任务
//（1）对于任务的持有方式，分为长持有和短持有；（默认为短持有策略）
//短持有：scr抢占到任务之后开始执行，同时开启该任务的续约，执行完成之后任务会被释放，对该任务的续约就会终止，下次执行时间到来之后再由各scr竞争获取该任务；
//长持有：scr抢占到任务之后开始执行，同时开启该任务的续约，执行完成之后不会被释放，一直续约下去，下次执行时间到来之后仍由这个scr执行，如果续约失败，其它scr会剥夺这个任务。
//（3）对于scr的管理，可以设置任务调度的降级策略：开启降级之后不会抢占新的任务，scr根据降级释放任务的策略释放手中持有的任务；提供了2种释放策略：
//**提示：对于负载均衡抢占策略+长持有策略，降级恢复后有机会触发负载再平衡（即刚恢复的scr从高负载scr处剥夺得到任务）
// 全部释放：开启降级后，scr立刻释放持有的全部任务
// 指定最大负载：开启降级后，要求本scr的负载不能高于指定负载，如果开启降级之前本scr的负载高于指定负载，则会释放一些任务直到本scr的负载低于指定负载，可以指定优先释放高负载任务或低负载任务
// 仅保留白名单任务（后续支持）：开启降级后，scr仅保留目前持有的在白名单中的任务，其余的任务都释放

// 任务的调度方式，持有方式和降级释放策略均可以自由组合配置

// 使用默认配置：普通抢占策略 & 任务短持有策略 & 全部释放的降级释放任务策略
func Example1() {
	ctx := context.Background()
	db := ioc.InitDB()
	//需要传入gorm.DB的对象指针
	scr := cs.NewScheduler(db)
	//添加job, 此方法会将job存入数据库，需要传入job名称，任务执行的超时时间，任务的cron表达式（精确到分钟），任务的权重
	err := scr.AddJob(ctx, "job1", time.Second*10, "*/1 * * * *", 70)
	if err != nil {
		return
	}
	//向scr注册job的执行方法
	scr.RegisterJobFunc("job1", func(ctx context.Context) error {
		fmt.Println("job1在执行")
		return nil
	})
	//开始调度，直到ctx.Done()可以取出信号时停止调度
	scr.Run(ctx)
	// Output:
}

func Example2() {
	ctx := context.Background()
	db := ioc.InitDB()
	//指定续约job和scr心跳的时间间隔，不指定默认为50s
	//指定判定job续约失败和scr下线的时间间隔，不指定默认为1分钟；如果一个被持有的job超过1分钟无人续约，则其它scr可以剥夺
	scr := cs.NewScheduler(db, cs.WithRefreshInterval(time.Second*10), cs.WithTimeoutInterval(time.Minute))
	//开始调度，直到ctx.Done()可以取出信号时停止调度
	scr.Run(ctx)
	// Output:
}

func Example4() {
	ctx := context.Background()
	db := ioc.InitDB()
	//指定持有的最多任务数量，如果达到了数量，发起抢占前会被阻塞
	scr := cs.NewScheduler(db, cs.WithMaxConcurrentPreemptNum(100))
	//开始调度，直到ctx.Done()可以取出信号时停止调度
	scr.Run(ctx)
	// Output:
}

// 使用负载均衡抢占策略 & 任务短持有策略（默认） & 全部释放的降级释放任务策略（默认）
func Example5() {
	ctx := context.Background()
	db := ioc.InitDB()
	//需要传入gorm.DB的对象指针
	scr := cs.NewScheduler(db, cs.WithLoadBalancePreemptStrategy())
	//开始调度，ctx.Done()可以取出信号时停止调度
	scr.Run(ctx)
	// Output:
}

// 使用普通抢占策略（默认） & 任务长持有策略 & 全部释放的降级释放任务策略（默认）
func Example6() {
	ctx := context.Background()
	db := ioc.InitDB()
	//需要传入gorm.DB的对象指针
	scr := cs.NewScheduler(db, cs.WithLongScheduleStrategy())
	//开始调度，ctx.Done()可以取出信号时停止调度
	scr.Run(ctx)
	// Output:
}

// 使用负载均衡抢占策略 & 任务长持有策略 & 全部释放的降级释放任务策略（默认）
func Example7() {
	ctx := context.Background()
	db := ioc.InitDB()
	//需要传入gorm.DB的对象指针
	scr := cs.NewScheduler(db, cs.WithLongScheduleStrategy(), cs.WithLoadBalancePreemptStrategy())
	//开始调度，ctx.Done()可以取出信号时停止调度
	scr.Run(ctx)
	// Output:
}

// 使用负载均衡抢占策略 & 任务长持有策略 & 指定最大负载的降级释放任务策略,指定最大负载为150，负载过高时优先释放高权重任务
func Example8() {
	ctx := context.Background()
	db := ioc.InitDB()
	//需要传入gorm.DB的对象指针
	scr := cs.NewScheduler(db, cs.WithLoadBalancePreemptStrategy(), cs.WithLongScheduleStrategy(), cs.WithLimitLoadDownGradeStrategy(150, cs.HighWeightFirst))
	scr.Run(ctx)
	// Output:
}

func Example9() {
	ctx := context.Background()
	db := ioc.InitDB()
	//需要传入gorm.DB的对象指针
	scr := cs.NewScheduler(db, cs.WithLoadBalancePreemptStrategy(), cs.WithLongScheduleStrategy(), cs.WithLimitLoadDownGradeStrategy(150, cs.HighWeightFirst))
	scr.Run(ctx)
	//开启降级
	scr.SetDownGrade(true)
	//降级恢复
	scr.SetDownGrade(false)
}
